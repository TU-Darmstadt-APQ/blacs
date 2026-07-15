"""Maintain an NI TTL output's thermal duty cycle between BLACS shots.

Set THERMAL_DEVICE_NAME and THERMAL_TTL_CHANNEL for the apparatus before
enabling this plugin in ``[BLACS/plugins]``.
"""

import logging
import time
from operator import index as integer_index

import labscript_utils.h5_lock  # noqa: F401 - required before importing h5py
import h5py
import labscript_utils.properties as properties

from qtutils import inmain, inmain_decorator
from qtutils.qt import QtCore, QtWidgets

from blacs.plugins import callback
from blacs.tab_base_classes import MODE_MANUAL, PluginTab

from .duty import (
    DutyHistory,
    duty_from_intervals,
    duty_from_trace,
    keep_warm_level,
    packed_ttl_levels,
    wait_duty_seconds,
)


# Apparatus configuration. Fill in these two strings before enabling the plugin.
THERMAL_DEVICE_NAME = 'pci_6534_1'
THERMAL_TTL_CHANNEL = 'port1/line0'  # for example: 'port0/line3'
ACTIVE_HIGH = True
# For unstructured two-dimensional DO tables, use this column instead of the
# numeric suffix of ``portN``. Leave as None for the usual port-number mapping.
THERMAL_DO_PORT_INDEX = None
# Fallback used only for legacy shot files lacking the connection-table ports
# property. Current NI_DAQmx shots derive offsets from that property instead.
NI_LINES_PER_PORT = 8
# Absolute HDF5 path to the timestamps for the NI DO table. Set this if the
# shot format does not use one of the automatic candidate paths below. The
# dataset must contain either one time per DO row or interval boundaries.
THERMAL_CLOCK_TIMES_DATASET = None

# Thermalisation policy.
IDLE_TIMEOUT_S = 30.0
KEEP_WARM_PERIOD_S = 10.0
DUTY_HISTORY_SIZE = 100
TIMER_INTERVAL_MS = 200
# Allow the queue manager to start a just-dequeued final shot, or requeue a
# repeat, before treating an empty model as an idle queue.
QUEUE_EMPTY_SETTLE_MS = 100
INTERRUPTION_SETTLE_S = 0.1


logger = logging.getLogger('BLACS.plugin.thermalization')


def _split_ttl_channel(channel):
    """Return the NI port name and line number for ``portN/lineM``."""
    try:
        port, line = channel.split('/line')
        line = int(line)
    except (AttributeError, TypeError, ValueError):
        raise ValueError(
            'THERMAL_TTL_CHANNEL must have the form portN/lineM, not %r' % channel
        )
    if not port or line < 0:
        raise ValueError('invalid NI TTL channel %r' % channel)
    return port, line


def _port_index(port):
    if THERMAL_DO_PORT_INDEX is not None:
        try:
            index = integer_index(THERMAL_DO_PORT_INDEX)
        except TypeError:
            raise ValueError('THERMAL_DO_PORT_INDEX must be an integer or None')
        if index < 0:
            raise ValueError('THERMAL_DO_PORT_INDEX must be non-negative')
        return index
    try:
        if not port.startswith('port'):
            raise ValueError
        return int(port[4:])
    except ValueError:
        raise ValueError(
            'set THERMAL_DO_PORT_INDEX for unstructured port %r' % port
        )


def _ttl_levels(do_table, port, line, packed_bit_index=None):
    """Return physical TTL levels from structured or packed NI DO tables."""
    if do_table.dtype.names is not None:
        if port not in do_table.dtype.names:
            raise RuntimeError('DO table has no field %r' % port)
        packed_values = do_table[port][:]
        bit_index = line
    else:
        values = do_table[:]
        if values.ndim == 1:
            packed_values = values
            bit_index = packed_bit_index
        elif values.ndim == 2:
            index = _port_index(port)
            if not 0 <= index < values.shape[1]:
                raise RuntimeError('DO table has no column for %s' % port)
            packed_values = values[:, index]
            bit_index = line
        else:
            raise RuntimeError('unsupported %d-dimensional DO table' % values.ndim)

    return packed_ttl_levels(packed_values, bit_index)


class Plugin(object):
    def __init__(self, initial_settings):
        self.menu = None
        self.notifications = {}
        self.BLACS = None
        self.tab = None
        self.timer = None
        self.empty_queue_timer = None

        self.history = DutyHistory(DUTY_HISTORY_SIZE)
        self.last_shot_duty = None
        self.state = 'Monitoring'
        self.detail = 'Waiting for the first completed shot'

        self.current_shot = None
        self.pending_shot = None
        self.keep_warm_active = False
        # Set from BLACS' queue pause button. The button is also updated by
        # ExperimentQueue.manager_paused, so this covers programmatic pauses.
        self.queue_paused = False
        self.queue_pause_started = None
        self.queue_empty = False
        self.queue_empty_started = None
        self.interruption_reason = None
        self.interruption_ready_at = None

    # BLACS plugin boilerplate:
    def get_menu_class(self):
        return None

    def get_notification_classes(self):
        return []

    def get_setting_classes(self):
        return []

    def set_menu_instance(self, menu):
        self.menu = menu

    def set_notification_instances(self, notifications):
        self.notifications = notifications

    def get_save_data(self):
        # Duty history deliberately resets when BLACS restarts.
        return {}

    def get_tab_classes(self):
        return {'Thermalization': ThermalizationTab}

    @inmain_decorator(True)
    def tabs_created(self, tabs_dict):
        self.tab = tabs_dict['Thermalization']
        self.tab.plugin = self

    def plugin_setup_complete(self, BLACS):
        self.BLACS = BLACS
        if self.tab is None:
            raise RuntimeError('Thermalization status tab was not created')
        try:
            queue_pause_button = BLACS['ui'].queue_pause_button
        except (KeyError, AttributeError):
            logger.warning('Could not find BLACS queue pause button')
        else:
            self.queue_paused = queue_pause_button.isChecked()
            queue_pause_button.toggled.connect(self._queue_pause_toggled)
        BLACS['ui'].queue_abort_button.clicked.connect(self._abort_requested)

        queue_model = BLACS['experiment_queue']._model
        self.queue_empty = queue_model.rowCount() == 0
        queue_model.rowsInserted.connect(self._queue_model_changed)
        queue_model.rowsRemoved.connect(self._queue_model_changed)
        queue_model.modelReset.connect(self._queue_model_changed)

        self.empty_queue_timer = QtCore.QTimer(self.tab._ui)
        self.empty_queue_timer.setSingleShot(True)
        self.empty_queue_timer.setInterval(QUEUE_EMPTY_SETTLE_MS)
        self.empty_queue_timer.timeout.connect(self._queue_empty_settled)

        self.timer = QtCore.QTimer(self.tab._ui)
        self.timer.setInterval(TIMER_INTERVAL_MS)
        self.timer.timeout.connect(self._on_timer_tick)
        self.timer.start()
        self._refresh_status()

    def get_callbacks(self):
        return {
            'pre_transition_to_buffered': self.pre_transition_to_buffered,
            'science_starting': self.science_starting,
            'science_over': self.science_over,
            'shot_complete': self.shot_complete,
        }

    def _set_status(self, state, detail):
        """Update the status model and its tab together."""
        self.state = state
        self.detail = detail
        self._refresh_status()

    def _clear_interruption(self):
        """Clear the pending-interruption marker and its retry deadline."""
        self.interruption_reason = None
        self.interruption_ready_at = None

    @callback(priority=-100)
    def pre_transition_to_buffered(self, h5_filepath):
        """End idle pulsing and prepare the next compiled shot's sample."""
        self._clear_interruption()
        was_keep_warm = self.keep_warm_active
        inmain(self._stop_keep_warm)
        self._finalise_pending_shot(time.monotonic())
        # The actual wait durations are written to HDF5 only while BLACS
        # transitions back to manual, so defer duty accounting to shot_complete.
        self.current_shot = {
            'path': h5_filepath,
            # Keep-warm can disturb the first real shot's effective duty. Its
            # programmed portion and following static interval are both ignored.
            'ignore': was_keep_warm,
        }
        self._set_status('Shot running', 'Prepared duty sample for queued shot')

    def science_starting(self, h5_filepath):
        if self.current_shot is not None:
            self._set_status(
                'Shot running', 'Thermal TTL is controlled by the buffered shot'
            )

    def science_over(self, h5_filepath):
        """Mark the beginning of the final static interval for this shot."""
        if self.current_shot is None:
            return
        self.current_shot['static_since'] = time.monotonic()

    @callback(priority=100)
    def shot_complete(self, h5_filepath):
        """Read the completed shot once its wait-monitor data has been saved."""
        if self.current_shot is None:
            return
        try:
            active_time, total_time, final_active = self._read_shot_duty(h5_filepath)
        except Exception as exc:
            self.current_shot = None
            self._set_error('Could not read completed thermal TTL duty: %s' % exc)
            return

        self.pending_shot = self.current_shot
        self.pending_shot.update(
            active_time=active_time,
            total_time=total_time,
            final_active=final_active,
            static_since=self.pending_shot.get('static_since', time.monotonic()),
        )
        self.current_shot = None
        self._set_status(
            'Monitoring', 'Recorded shot duty including measured waits'
        )

        # A queue pause does not interrupt a currently running shot. Once it
        # has completed and BLACS is back in manual mode, finalise it at the
        # pause boundary and begin pulsing without waiting for the idle timer.
        if self.queue_paused:
            self._begin_keep_warm_after_queue_idle()
        elif self.queue_empty:
            self._schedule_keep_warm_for_empty_queue()

    def _queue_pause_toggled(self, paused):
        """React immediately when BLACS pauses or resumes the queue.

        ``manager_paused`` drives this same button, so the signal covers both
        user clicks and pauses requested by other BLACS code.
        """
        self.queue_paused = bool(paused)
        if not self.queue_paused:
            self.queue_pause_started = None
            if self.keep_warm_active:
                self._set_status(
                    self.state,
                    'Queue resumed; keep-warm continues until the next shot',
                )
            return

        self.queue_pause_started = time.monotonic()
        if self.current_shot is not None:
            if self._has_device_error():
                self._begin_keep_warm_after_interruption('Device error')
                return
            self._set_status(
                'Shot running',
                'Queue paused; keep-warm begins when the current shot completes',
            )
            return
        self._begin_keep_warm_after_queue_idle()

    @inmain_decorator(True)
    def _begin_keep_warm_after_queue_idle(self):
        """Exclude a pause or empty queue from the current sample promptly."""
        # A queued pause can arrive while shot-complete is still unwinding.
        # Do not pulse until the active shot has relinquished the buffered DO.
        if self.current_shot is not None:
            return
        if self.pending_shot is None:
            reason = 'Queue paused' if self.queue_paused else 'Queue empty'
            self._set_status(
                'Monitoring', reason + '; no completed shot is available yet'
            )
            return
        if self.keep_warm_active:
            return

        # If a pause or queue-empty event occurs just after science_over but
        # before shot_complete has parsed the measured waits, keep only the
        # preceding static interval. An event during a real shot must not
        # truncate it, hence the lower bound at static_since.
        idle_started = [
            timestamp
            for timestamp in (
                self.queue_pause_started,
                self.queue_empty_started,
            )
            if timestamp is not None
        ]
        sample_end = max(
            self.pending_shot['static_since'],
            min(idle_started) if idle_started else time.monotonic(),
        )
        self._finalise_pending_shot(sample_end)
        self._start_keep_warm(time.monotonic())

    def _queue_model_changed(self, *args):
        """Track whether queued work remains without touching hardware yet."""
        is_empty = self.BLACS['experiment_queue']._model.rowCount() == 0
        if is_empty:
            if not self.queue_empty:
                self.queue_empty_started = time.monotonic()
            self.queue_empty = True
            # The final row is removed before its shot starts. Defer the
            # decision briefly so pre_transition_to_buffered can mark it as
            # active, and so repeat mode can reinsert it.
            if self.pending_shot is not None:
                self._schedule_keep_warm_for_empty_queue()
            return

        self.queue_empty = False
        self.queue_empty_started = None

    @inmain_decorator(True)
    def _schedule_keep_warm_for_empty_queue(self):
        if self.empty_queue_timer is not None:
            self.empty_queue_timer.start()

    def _queue_empty_settled(self):
        if (
            self.queue_empty
            and self.current_shot is None
            and self.pending_shot is not None
            and not self.keep_warm_active
        ):
            self._begin_keep_warm_after_queue_idle()

    def _has_device_error(self):
        """Return whether a BLACS device tab reports an active error."""
        try:
            tablist = self.BLACS['experiment_queue'].BLACS.tablist
            return any(
                tab.error_message or tab.state == 'fatal error'
                for tab in tablist.values()
            )
        except (AttributeError, KeyError):
            return False

    def _queue_interruption_reason(self):
        """Interpret terminal QueueManager states that end the current shot."""
        try:
            status = self.BLACS['experiment_queue'].get_status().lower()
        except (AttributeError, KeyError):
            return None
        if 'restarted' in status:
            return 'Device restarted'
        if 'aborted' in status:
            return 'Experiment aborted'
        if 'timed out' in status:
            return 'Experiment timed out'
        if 'error' in status or 'failed' in status:
            return 'Experiment failed'
        return None

    def _abort_requested(self):
        """Mark an abort now; start only if BLACS does not run another shot."""
        self._begin_keep_warm_after_interruption('Experiment aborted')

    def _begin_keep_warm_after_interruption(self, reason):
        """Discard an interrupted shot and await manual control of the NI tab."""
        if self.interruption_reason is not None or self.keep_warm_active:
            return

        now = time.monotonic()
        # An interrupted shot has no trustworthy recorded duration, so it
        # must never enter the duty history. A preceding completed shot can
        # still be finalised exactly at the device-error boundary.
        self.current_shot = None
        self._finalise_pending_shot(now)
        self.interruption_reason = reason
        self.interruption_ready_at = now + INTERRUPTION_SETTLE_S
        self._set_status(
            'Error', '%s; waiting for thermal NI manual mode' % reason
        )

    def _try_keep_warm_after_interruption(self, now):
        """Start pulsing only after recovery has returned the target to manual."""
        if self.interruption_reason is None:
            return
        if now < self.interruption_ready_at:
            return
        try:
            target_tab = self._target_tab()
        except Exception as exc:
            self._set_error_main(
                'Could not access thermal NI after interruption: %s' % exc
            )
            return
        if target_tab.mode != MODE_MANUAL:
            self._set_status(
                'Error',
                '%s; waiting for thermal NI manual mode'
                % self.interruption_reason,
            )
            return

        self._clear_interruption()
        self._start_keep_warm(now)

    def _read_shot_duty(self, h5_filepath):
        """Read the packed NI values and their timestamps directly from HDF5."""
        with h5py.File(h5_filepath, 'r') as h5_file:
            values = self._read_ttl_levels(h5_file)
            times = self._read_clock_times(h5_file, len(values))
            wait_times, wait_durations = self._read_waits(h5_file)

        active_time, total_time, value_times = self._trace_duty(times, values)
        wait_active_time, wait_total_time = wait_duty_seconds(
            value_times,
            values,
            wait_times,
            wait_durations,
            active_high=ACTIVE_HIGH,
        )
        return (
            active_time + wait_active_time,
            total_time + wait_total_time,
            bool(values[-1]) == ACTIVE_HIGH,
        )

    def _read_ttl_levels(self, h5_file):
        """Read the configured physical TTL from an NI digital-output table."""
        port, line = _split_ttl_channel(THERMAL_TTL_CHANNEL)
        try:
            do_table = h5_file['devices'][THERMAL_DEVICE_NAME]['DO']
        except KeyError:
            raise RuntimeError(
                'no DO table for %s on %s'
                % (THERMAL_TTL_CHANNEL, THERMAL_DEVICE_NAME)
            )

        packed_bit_index = None
        if do_table.dtype.names is None and do_table.ndim == 1:
            packed_bit_index = self._packed_bit_index(h5_file, port, line)
        values = _ttl_levels(do_table, port, line, packed_bit_index)
        if not values:
            raise RuntimeError('DO table for %s is empty' % THERMAL_DEVICE_NAME)
        return values

    @staticmethod
    def _trace_duty(times, values):
        """Return programmed duty seconds and the timestamp for each DO value."""
        if len(times) == len(values):
            active_time, total_time = duty_from_trace(
                times, values, active_high=ACTIVE_HIGH
            )
            value_times = times
        elif len(times) == len(values) + 1:
            active_time, total_time = duty_from_intervals(
                times, values, active_high=ACTIVE_HIGH
            )
            value_times = times[:-1]
        else:
            raise RuntimeError(
                'clock-times dataset has %d samples for %d DO rows'
                % (len(times), len(values))
            )
        return active_time, total_time, value_times

    @staticmethod
    def _read_waits(h5_file):
        """Return wait-monitor timestamps and their measured durations."""
        if 'data/waits' not in h5_file:
            return [], []
        waits = h5_file['data/waits'][:]
        field_names = waits.dtype.names or ()
        if 'time' not in field_names or 'duration' not in field_names:
            raise RuntimeError('data/waits lacks time or duration fields')
        return waits['time'], waits['duration']

    @staticmethod
    def _read_clock_times(h5_file, do_row_count):
        """Load direct timing data for the NI DO table from the shot file."""
        if THERMAL_CLOCK_TIMES_DATASET is not None:
            candidate_paths = [THERMAL_CLOCK_TIMES_DATASET]
        else:
            device_path = 'devices/%s' % THERMAL_DEVICE_NAME
            candidate_paths = [
                device_path + '/TIMES',
                device_path + '/CLOCK_TIMES',
                device_path + '/clock_times',
                device_path + '/times',
            ]

        for path in candidate_paths:
            if path in h5_file:
                times = h5_file[path][:]
                if times.ndim != 1:
                    raise RuntimeError(
                        'clock-times dataset %r must be one-dimensional' % path
                    )
                if len(times) in (do_row_count, do_row_count + 1):
                    return times
                raise RuntimeError(
                    'clock-times dataset %r has %d samples for %d DO rows'
                    % (path, len(times), do_row_count)
                )

        raise RuntimeError(
            'no direct clock-times dataset found; set THERMAL_CLOCK_TIMES_DATASET'
        )

    @staticmethod
    def _packed_bit_index(h5_file, port_name, line):
        """Find a packed DO bit offset using NI_DAQmx's stored port layout."""
        try:
            connection_properties = properties.get(
                h5_file, THERMAL_DEVICE_NAME, 'connection_table_properties'
            )
            ports = connection_properties['ports']
        except (KeyError, TypeError, AttributeError):
            # Older NI_DAQmx files packed the usual four byte-wide ports.
            if line >= NI_LINES_PER_PORT:
                raise RuntimeError(
                    'line%d is outside legacy %s layout' % (line, port_name)
                )
            return _port_index(port_name) * NI_LINES_PER_PORT + line

        try:
            port_items = ports.items()
        except AttributeError:
            raise RuntimeError('connection-table ports metadata is malformed')

        offset = 0
        for name, port in port_items:
            try:
                supports_buffered = port['supports_buffered']
                num_lines = integer_index(port['num_lines'])
            except (KeyError, TypeError):
                raise RuntimeError(
                    'connection-table metadata for %s is malformed' % name
                )
            if num_lines < 0:
                raise RuntimeError(
                    'connection-table metadata for %s is malformed' % name
                )
            if not supports_buffered:
                continue
            if name == port_name:
                if not 0 <= line < num_lines:
                    raise RuntimeError('line%d is outside %s' % (line, port_name))
                return offset + line
            offset += num_lines
        raise RuntimeError('%s is not a buffered NI port' % port_name)

    def _finalise_pending_shot(self, now):
        shot = self.pending_shot
        if shot is None:
            return
        static_duration = max(0.0, now - shot['static_since'])
        shot['total_time'] += static_duration
        if shot['final_active']:
            shot['active_time'] += static_duration
        self.pending_shot = None

        if shot['ignore']:
            detail = 'Ignored first shot after keep-warm'
        else:
            self.last_shot_duty = self.history.append_seconds(
                shot['active_time'], shot['total_time']
            )
            detail = 'Recorded completed shot duty'
        self._set_status('Monitoring', detail)

    def _on_timer_tick(self):
        now = time.monotonic()
        if not self.keep_warm_active:
            reason = self._queue_interruption_reason()
            if reason is None and self._has_device_error():
                reason = 'Device error'
            if reason is not None:
                self._begin_keep_warm_after_interruption(reason)
        if self.interruption_reason is not None:
            self._try_keep_warm_after_interruption(now)
            return

        if not self.keep_warm_active:
            if self.pending_shot is None:
                self._refresh_status()
                return
            idle_elapsed = max(0.0, now - self.pending_shot['static_since'])
            if idle_elapsed < IDLE_TIMEOUT_S:
                self._set_status(
                    'Monitoring',
                    'Keep-warm starts after %.1f s idle'
                    % (IDLE_TIMEOUT_S - idle_elapsed),
                )
                return
            self._finalise_pending_shot(now)
            self._start_keep_warm(now)
            return

        duty = self.history.mean
        if duty is None:
            self._set_error_main('No duty-cycle history available for keep-warm')
            return
        elapsed = now - self.keep_warm_started
        phase = elapsed % KEEP_WARM_PERIOD_S
        should_be_active = keep_warm_level(duty, elapsed, KEEP_WARM_PERIOD_S)
        if should_be_active != self.keep_warm_output_active:
            try:
                self._set_target_active(should_be_active)
            except Exception as exc:
                self._set_error_main('Could not pulse thermal TTL: %s' % exc)
                return
            self.keep_warm_output_active = should_be_active
        self._set_status(
            'Keep-warm',
            'Cycle resets in %.1f s' % (KEEP_WARM_PERIOD_S - phase),
        )

    def _start_keep_warm(self, now):
        duty = self.history.mean
        if duty is None:
            self._set_error_main('No duty-cycle history available for keep-warm')
            return
        self.keep_warm_active = True
        self.keep_warm_started = now
        self.keep_warm_output_active = None
        self._set_status(
            'Keep-warm', 'Pulsing at %.1f%% duty' % (100.0 * duty)
        )
        self._on_timer_tick()

    def _stop_keep_warm(self):
        self.keep_warm_active = False
        self.keep_warm_output_active = None

    def _target_tab(self):
        try:
            return self.BLACS['experiment_queue'].BLACS.tablist[THERMAL_DEVICE_NAME]
        except KeyError:
            raise RuntimeError(
                'configured NI tab %r is not available' % THERMAL_DEVICE_NAME
            )

    @inmain_decorator(True)
    def _set_target_active(self, active):
        """Write the manual output from the Qt thread that owns the DeviceTab."""
        tab = self._target_tab()
        if tab.mode != MODE_MANUAL:
            raise RuntimeError('target tab is not in manual mode')
        channel = tab.get_channel(THERMAL_TTL_CHANNEL)
        if channel is None:
            raise RuntimeError('configured TTL channel is not available')
        physical_level = bool(active) if ACTIVE_HIGH else not bool(active)
        channel.set_value(physical_level, program=True)

    def _set_error(self, message):
        logger.error(message)
        inmain(self._set_error_main, message)

    def _set_error_main(self, message):
        self.keep_warm_active = False
        self.keep_warm_output_active = None
        self._clear_interruption()
        self._set_status('Error', message)

    @inmain_decorator(True)
    def _refresh_status(self):
        if self.tab is not None:
            self.tab.update_status(
                self.state,
                self.history.mean,
                self.history.count,
                self.last_shot_duty,
                self.detail,
            )

    def close(self):
        if self.timer is not None:
            inmain(self.timer.stop)
        if self.empty_queue_timer is not None:
            inmain(self.empty_queue_timer.stop)


class ThermalizationTab(PluginTab):
    def initialise_GUI(self):
        self.plugin = None
        layout = self.get_tab_layout()
        self.state_label = QtWidgets.QLabel('Starting…')
        self.mean_label = QtWidgets.QLabel('—')
        self.samples_label = QtWidgets.QLabel('0 / %d' % DUTY_HISTORY_SIZE)
        self.last_shot_label = QtWidgets.QLabel('—')
        self.detail_label = QtWidgets.QLabel('')
        self.detail_label.setWordWrap(True)

        form = QtWidgets.QFormLayout()
        form.addRow('Status:', self.state_label)
        form.addRow('Mean duty cycle:', self.mean_label)
        form.addRow('Samples:', self.samples_label)
        form.addRow('Last shot:', self.last_shot_label)
        form.addRow('Detail:', self.detail_label)
        layout.addLayout(form)
        layout.addStretch(1)

    def update_status(self, state, mean, count, last_shot, detail):
        self.state_label.setText(state)
        self.mean_label.setText('—' if mean is None else '%.2f%%' % (100.0 * mean))
        self.samples_label.setText('%d / %d' % (count, DUTY_HISTORY_SIZE))
        self.last_shot_label.setText(
            '—' if last_shot is None else '%.2f%%' % (100.0 * last_shot)
        )
        self.detail_label.setText(detail)

    def get_save_data(self):
        return {}

    def restore_save_data(self, data):
        return
