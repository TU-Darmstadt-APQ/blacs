"""Maintain an NI TTL output's thermal duty cycle between BLACS shots.

Set THERMAL_DEVICE_NAME and THERMAL_TTL_CHANNEL for the apparatus before
enabling this plugin in ``[BLACS/plugins]``.
"""

import logging
import time

import labscript_utils.h5_lock  # noqa: F401 - required before importing h5py
import h5py

from qtutils import inmain, inmain_decorator
from qtutils.qt import QtCore, QtWidgets

from blacs.plugins import callback
from blacs.tab_base_classes import MODE_MANUAL, PluginTab

from .duty import DutyHistory, duty_from_trace, keep_warm_level


# Apparatus configuration. Fill in these two strings before enabling the plugin.
THERMAL_DEVICE_NAME = 'SET_NI_DEVICE_NAME'
THERMAL_TTL_CHANNEL = 'SET_PORT_AND_LINE'  # for example: 'port0/line3'
ACTIVE_HIGH = True

# Thermalisation policy.
IDLE_TIMEOUT_S = 30.0
KEEP_WARM_PERIOD_S = 10.0
DUTY_HISTORY_SIZE = 100
TIMER_INTERVAL_MS = 200


logger = logging.getLogger('BLACS.plugin.thermalization')


def _as_text(value):
    return value.decode() if isinstance(value, bytes) else str(value)


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


class Plugin(object):
    def __init__(self, initial_settings):
        self.menu = None
        self.notifications = {}
        self.BLACS = None
        self.tab = None
        self.timer = None

        self.history = DutyHistory(DUTY_HISTORY_SIZE)
        self.last_shot_duty = None
        self.state = 'Monitoring'
        self.detail = 'Waiting for the first completed shot'

        self.current_shot = None
        self.finished_shot = None
        self.pending_shot = None
        self.keep_warm_active = False

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

    @callback(priority=-100)
    def pre_transition_to_buffered(self, h5_filepath):
        """End idle pulsing and prepare the next compiled shot's sample."""
        was_keep_warm = self.keep_warm_active
        inmain(self._stop_keep_warm)
        self._finalise_pending_shot(time.monotonic())
        # A science_over without shot_complete belongs to an aborted shot.
        self.finished_shot = None

        try:
            # runviewer.Shot walks Qt-managed connection-table objects while it
            # reconstructs a trace. Queue callbacks run in the queue-manager
            # thread, so dispatch the complete reconstruction to the GUI thread.
            active_time, total_time = inmain(self._read_shot_duty, h5_filepath)
        except Exception as exc:
            self.current_shot = None
            self._set_error('Could not read thermal TTL duty: %s' % exc)
            return

        self.current_shot = {
            'path': h5_filepath,
            'active_time': active_time,
            'total_time': total_time,
            # Keep-warm can disturb the first real shot's effective duty. Its
            # programmed portion and following static interval are both ignored.
            'ignore': was_keep_warm,
        }
        self.state = 'Shot running'
        self.detail = 'Prepared duty sample for queued shot'
        self._refresh_status()

    def science_starting(self, h5_filepath):
        if self.current_shot is not None:
            self.state = 'Shot running'
            self.detail = 'Thermal TTL is controlled by the buffered shot'
            self._refresh_status()

    def science_over(self, h5_filepath):
        """Capture the final TTL value at the start of the manual transition."""
        if self.current_shot is None:
            return
        try:
            # This callback runs in the queue-manager thread. Read the compiled
            # final value from HDF5 rather than reading DeviceTab state, which is
            # owned by the Qt main thread.
            final_active = self._target_final_active_from_shot(h5_filepath)
        except Exception as exc:
            self.finished_shot = None
            self._set_error('Could not read final thermal TTL value: %s' % exc)
            return

        self.finished_shot = self.current_shot.copy()
        self.finished_shot['final_active'] = final_active
        self.finished_shot['static_since'] = time.monotonic()
        self.current_shot = None

    @callback(priority=100)
    def shot_complete(self, h5_filepath):
        """Only successful shots become eligible for the duty history."""
        if self.finished_shot is None:
            return
        self.pending_shot = self.finished_shot
        self.finished_shot = None
        self.state = 'Monitoring'
        self.detail = 'Including final TTL value while BLACS is idle'
        self._refresh_status()

    def _read_shot_duty(self, h5_filepath):
        """Use runviewer to reconstruct the NI trace from the shot program."""
        try:
            from runviewer.__main__ import Shot
        except ImportError as exc:
            raise RuntimeError('runviewer is required to reconstruct NI timing') from exc

        trace_name = self._target_trace_name(h5_filepath)
        shot = Shot(h5_filepath)
        # Recent runviewer releases construct traces lazily; older releases do
        # so in Shot.__init__. Supporting both avoids a version-specific parser.
        get_traces = getattr(shot, 'get_traces', None)
        if get_traces is not None:
            get_traces()
        traces = shot.traces
        if trace_name not in traces:
            raise RuntimeError('runviewer did not produce trace %r' % trace_name)
        times, values = traces[trace_name]
        return duty_from_trace(times, values, active_high=ACTIVE_HIGH)

    @staticmethod
    def _target_trace_name(h5_filepath):
        """Map the configured NI hardware channel to its labscript output name."""
        with h5py.File(h5_filepath, 'r') as h5_file:
            table = h5_file['connection table']
            for row in table:
                parent = _as_text(row['parent'])
                parent_port = _as_text(row['parent port'])
                if parent == THERMAL_DEVICE_NAME and parent_port == THERMAL_TTL_CHANNEL:
                    return _as_text(row['name'])
        raise RuntimeError(
            'no connection-table output for %s on %s'
            % (THERMAL_TTL_CHANNEL, THERMAL_DEVICE_NAME)
        )

    @staticmethod
    def _target_final_active_from_shot(h5_filepath):
        """Read the final packed NI digital value without touching the GUI tab."""
        port, line = _split_ttl_channel(THERMAL_TTL_CHANNEL)
        with h5py.File(h5_filepath, 'r') as h5_file:
            try:
                do_table = h5_file['devices'][THERMAL_DEVICE_NAME]['DO']
                final_port_value = int(do_table[port][-1])
            except (KeyError, IndexError, TypeError):
                raise RuntimeError('target final value is unavailable in the shot DO table')
        physical_level = bool(final_port_value & (1 << line))
        return physical_level == ACTIVE_HIGH

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
            self.detail = 'Ignored first shot after keep-warm'
        else:
            self.last_shot_duty = self.history.append_seconds(
                shot['active_time'], shot['total_time']
            )
            self.detail = 'Recorded completed shot duty'
        self.state = 'Monitoring'
        self._refresh_status()

    def _on_timer_tick(self):
        now = time.monotonic()
        if not self.keep_warm_active:
            if self.pending_shot is None:
                self._refresh_status()
                return
            idle_elapsed = max(0.0, now - self.pending_shot['static_since'])
            if idle_elapsed < IDLE_TIMEOUT_S:
                self.state = 'Monitoring'
                self.detail = 'Keep-warm starts after %.1f s idle' % (
                    IDLE_TIMEOUT_S - idle_elapsed
                )
                self._refresh_status()
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
        self.state = 'Keep-warm'
        self.detail = 'Cycle resets in %.1f s' % (KEEP_WARM_PERIOD_S - phase)
        self._refresh_status()

    def _start_keep_warm(self, now):
        duty = self.history.mean
        if duty is None:
            self._set_error_main('No duty-cycle history available for keep-warm')
            return
        self.keep_warm_active = True
        self.keep_warm_started = now
        self.keep_warm_output_active = None
        self.state = 'Keep-warm'
        self.detail = 'Pulsing at %.1f%% duty' % (100.0 * duty)
        self._on_timer_tick()

    def _stop_keep_warm(self):
        self.keep_warm_active = False
        self.keep_warm_output_active = None

    def _target_tab(self):
        try:
            return self.BLACS['experiment_queue'].BLACS.tablist[THERMAL_DEVICE_NAME]
        except KeyError:
            raise RuntimeError('configured NI tab %r is not available' % THERMAL_DEVICE_NAME)

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
        self.state = 'Error'
        self.detail = message
        self._refresh_status()

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
