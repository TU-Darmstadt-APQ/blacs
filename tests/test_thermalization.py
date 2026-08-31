import importlib.util
import sys
import types
import unittest
from collections import OrderedDict
from pathlib import Path
from unittest import mock


MODULE_PATH = Path(__file__).parents[1] / 'blacs/plugins/thermalization/duty.py'
SPEC = importlib.util.spec_from_file_location('thermalization_duty', MODULE_PATH)
thermalization_duty = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(thermalization_duty)
DutyHistory = thermalization_duty.DutyHistory
duty_from_trace = thermalization_duty.duty_from_trace
duty_from_intervals = thermalization_duty.duty_from_intervals
keep_warm_level = thermalization_duty.keep_warm_level
keep_warm_transition_delay = thermalization_duty.keep_warm_transition_delay
packed_ttl_levels = thermalization_duty.packed_ttl_levels
wait_duty_seconds = thermalization_duty.wait_duty_seconds


def load_plugin_module():
    """Load the plugin with small dependency stubs for state-machine tests."""
    package_name = 'thermalization_plugin_under_test'
    package_path = MODULE_PATH.parent

    labscript_utils = types.ModuleType('labscript_utils')
    h5_lock = types.ModuleType('labscript_utils.h5_lock')
    properties = types.ModuleType('labscript_utils.properties')
    properties.get = mock.Mock()
    labscript_utils.h5_lock = h5_lock
    labscript_utils.properties = properties

    h5py = types.ModuleType('h5py')
    h5py.File = mock.Mock()

    qtutils = types.ModuleType('qtutils')
    qtutils.inmain = lambda function, *args, **kwargs: function(*args, **kwargs)
    qtutils.inmain_decorator = lambda *args, **kwargs: lambda function: function
    qt = types.ModuleType('qtutils.qt')
    qt.QtCore = types.SimpleNamespace()
    qt.QtWidgets = types.SimpleNamespace()
    qtutils.qt = qt

    blacs = types.ModuleType('blacs')
    blacs.__path__ = []
    blacs_plugins = types.ModuleType('blacs.plugins')
    blacs_plugins.callback = lambda *args, **kwargs: lambda function: function
    tab_base_classes = types.ModuleType('blacs.tab_base_classes')
    tab_base_classes.MODE_MANUAL = 'manual'
    tab_base_classes.PluginTab = type('PluginTab', (object,), {})

    stubs = {
        'labscript_utils': labscript_utils,
        'labscript_utils.h5_lock': h5_lock,
        'labscript_utils.properties': properties,
        'h5py': h5py,
        'qtutils': qtutils,
        'qtutils.qt': qt,
        'blacs': blacs,
        'blacs.plugins': blacs_plugins,
        'blacs.tab_base_classes': tab_base_classes,
    }
    spec = importlib.util.spec_from_file_location(
        package_name,
        package_path / '__init__.py',
        submodule_search_locations=[str(package_path)],
    )
    module = importlib.util.module_from_spec(spec)
    with mock.patch.dict(sys.modules, stubs):
        sys.modules[package_name] = module
        spec.loader.exec_module(module)
    return module


thermalization_plugin = load_plugin_module()


class FakeDtype(object):
    def __init__(self, names=None):
        self.names = names


class FakeArray(object):
    def __init__(self, data, ndim=None):
        self.data = data
        if ndim is None:
            ndim = 2 if data and isinstance(data[0], (list, tuple)) else 1
        self.ndim = ndim
        self.shape = (
            (len(data), len(data[0])) if ndim == 2 else (len(data),)
        )

    def __getitem__(self, key):
        if isinstance(key, tuple):
            rows, column = key
            return [row[column] for row in self.data[rows]]
        if isinstance(key, slice):
            return FakeArray(self.data[key], self.ndim)
        return self.data[key]

    def __iter__(self):
        return iter(self.data)

    def __len__(self):
        return len(self.data)


class FakeDataset(FakeArray):
    def __init__(self, data, names=None, fields=None, ndim=None):
        super(FakeDataset, self).__init__(data, ndim=ndim)
        self.dtype = FakeDtype(names)
        self.fields = fields or {}

    def __getitem__(self, key):
        if isinstance(key, str):
            return FakeArray(self.fields[key])
        return super(FakeDataset, self).__getitem__(key)


class FakeStructuredRows(object):
    def __init__(self, fields, names):
        self.fields = fields
        self.dtype = FakeDtype(names)

    def __getitem__(self, key):
        if isinstance(key, slice):
            return self
        return self.fields[key]


class FakeH5File(dict):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False


class TestDutyFromTrace(unittest.TestCase):
    def test_constant_high(self):
        self.assertEqual(duty_from_trace([0, 5], [1, 1]), (5.0, 5.0))

    def test_constant_low(self):
        self.assertEqual(duty_from_trace([0, 5], [0, 0]), (0.0, 5.0))

    def test_multi_pulse_trace(self):
        active, total = duty_from_trace([0, 2, 5, 8], [1, 0, 1, 1])
        self.assertEqual(total, 8.0)
        self.assertEqual(active, 5.0)

    def test_active_low(self):
        self.assertEqual(duty_from_trace([0, 4], [0, 0], active_high=False), (4.0, 4.0))

    def test_explicit_interval_boundaries(self):
        active, total = duty_from_intervals([0, 2, 5, 8], [1, 0, 1])
        self.assertEqual(total, 8.0)
        self.assertEqual(active, 5.0)

    def test_rejects_mismatched_and_non_monotonic_inputs(self):
        with self.assertRaisesRegex(ValueError, 'same length'):
            duty_from_trace([0, 1], [True])
        with self.assertRaisesRegex(ValueError, 'at least two'):
            duty_from_trace([0], [True])
        with self.assertRaisesRegex(ValueError, 'monotonic'):
            duty_from_trace([0, 2, 1], [True, False, True])
        with self.assertRaisesRegex(ValueError, 'one more item'):
            duty_from_intervals([0, 1], [True, False])
        with self.assertRaisesRegex(ValueError, 'monotonic'):
            duty_from_intervals([0, 2, 1], [True, False])

    def test_rejects_timelines_without_elapsed_time(self):
        with self.assertRaisesRegex(ValueError, 'no elapsed time'):
            duty_from_trace([1, 1], [True, True])
        with self.assertRaisesRegex(ValueError, 'no elapsed time'):
            duty_from_intervals([1], [])
        with self.assertRaisesRegex(ValueError, 'finite'):
            duty_from_trace([0, float('nan')], [True, True])


class TestDutyHistory(unittest.TestCase):
    def test_arithmetic_mean_and_bounded_history(self):
        history = DutyHistory(2)
        history.append_seconds(1, 4)
        history.append_seconds(3, 4)
        self.assertEqual(history.mean, 0.5)
        history.append_seconds(4, 4)
        self.assertEqual(history.count, 2)
        self.assertEqual(history.mean, 0.875)

    def test_final_static_interval_changes_a_shot_sample(self):
        history = DutyHistory(100)
        # 50% in the shot, then 4 s held high while BLACS is between shots.
        self.assertEqual(history.append_seconds(2 + 4, 4 + 4), 0.75)

    def test_rejects_invalid_history_and_samples(self):
        for size in (0, -1, 1.5):
            with self.subTest(size=size):
                with self.assertRaises(ValueError):
                    DutyHistory(size)
        history = DutyHistory(1)
        with self.assertRaisesRegex(ValueError, 'positive'):
            history.append_seconds(0, 0)
        with self.assertRaisesRegex(ValueError, 'within'):
            history.append_seconds(2, 1)
        with self.assertRaisesRegex(ValueError, 'finite'):
            history.append_seconds(1, float('inf'))


class TestKeepWarmLevel(unittest.TestCase):
    def test_half_duty_cycle(self):
        self.assertTrue(keep_warm_level(0.4, 3.9, 10))
        self.assertFalse(keep_warm_level(0.4, 4.0, 10))
        self.assertTrue(keep_warm_level(0.4, 13.9, 10))

    def test_zero_and_full_duty_cycle(self):
        self.assertFalse(keep_warm_level(0.0, 0.0, 10))
        self.assertFalse(keep_warm_level(0.0, 9.9, 10))
        self.assertTrue(keep_warm_level(1.0, 0.0, 10))
        self.assertTrue(keep_warm_level(1.0, 9.9, 10))

    def test_rejects_non_finite_cycle_values(self):
        with self.assertRaisesRegex(ValueError, 'finite'):
            keep_warm_level(0.5, float('inf'), 10)

    def test_transition_delay_tracks_cycle_edges(self):
        self.assertEqual(keep_warm_transition_delay(0.4, 0.0, 10), 4.0)
        self.assertAlmostEqual(
            keep_warm_transition_delay(0.4, 3.9, 10), 0.1
        )
        self.assertEqual(keep_warm_transition_delay(0.4, 4.0, 10), 6.0)
        self.assertAlmostEqual(
            keep_warm_transition_delay(0.4, 13.9, 10), 0.1
        )

    def test_constant_duties_have_no_transition(self):
        self.assertIsNone(keep_warm_transition_delay(0.0, 5.0, 10))
        self.assertIsNone(keep_warm_transition_delay(1.0, 5.0, 10))


class TestPackedTTLLevels(unittest.TestCase):
    def test_four_ports_are_combined_in_a_uint32(self):
        # port1/line0 is bit 8 when each port occupies one byte.
        self.assertEqual(packed_ttl_levels([0x00000000, 0x00000100, 0x00000001], 8),
                         [False, True, False])

    def test_rejects_invalid_bit_indexes(self):
        for bit_index in (-1, None, 1.5):
            with self.subTest(bit_index=bit_index):
                with self.assertRaises(ValueError):
                    packed_ttl_levels([0], bit_index)


class TestWaitDutySeconds(unittest.TestCase):
    def test_wait_duration_uses_ttl_value_at_wait_time(self):
        active, total = wait_duty_seconds(
            [0, 2, 5], [False, True, False], [1, 3, 5], [4, 6, 2]
        )
        self.assertEqual((active, total), (6.0, 12.0))

    def test_waits_before_at_and_after_trace_boundaries(self):
        active, total = wait_duty_seconds(
            [1, 3], [False, True], [0, 1, 3, 5], [1, 2, 4, 8]
        )
        self.assertEqual((active, total), (12.0, 15.0))

    def test_active_low_waits(self):
        self.assertEqual(
            wait_duty_seconds(
                [0, 2], [False, True], [1, 3], [4, 6], active_high=False
            ),
            (4.0, 10.0),
        )

    def test_rejects_malformed_wait_inputs(self):
        with self.assertRaisesRegex(ValueError, 'one time for each value'):
            wait_duty_seconds([], [], [], [])
        with self.assertRaisesRegex(ValueError, 'same length'):
            wait_duty_seconds([0], [True], [0], [])
        with self.assertRaisesRegex(ValueError, 'monotonic'):
            wait_duty_seconds([1, 0], [True, False], [], [])
        with self.assertRaisesRegex(ValueError, 'must not be negative'):
            wait_duty_seconds([0], [True], [0], [-1])
        with self.assertRaisesRegex(ValueError, 'finite'):
            wait_duty_seconds([0], [True], [0], [float('nan')])


class TestPluginStateMachine(unittest.TestCase):
    def setUp(self):
        self.plugin = thermalization_plugin.Plugin({})

    def set_pending_shot(
        self, active_time=1.0, total_time=2.0, final_active=False, ignore=False
    ):
        self.plugin.pending_shot = {
            'active_time': active_time,
            'total_time': total_time,
            'final_active': final_active,
            'static_since': 10.0,
            'ignore': ignore,
        }

    def test_completed_shot_includes_final_static_interval(self):
        self.plugin.current_shot = {
            'path': 'shot.h5',
            'ignore': False,
            'static_since': 10.0,
        }
        self.plugin._read_shot_duty = mock.Mock(return_value=(2.0, 4.0, True))

        self.plugin.shot_complete('shot.h5')
        self.plugin._finalise_pending_shot(14.0)

        self.assertEqual(self.plugin.last_shot_duty, 0.75)
        self.assertEqual(self.plugin.history.mean, 0.75)

    def test_first_shot_after_keep_warm_is_ignored(self):
        self.set_pending_shot(ignore=True)
        self.plugin._finalise_pending_shot(12.0)
        self.assertEqual(self.plugin.history.count, 0)
        self.assertIsNone(self.plugin.last_shot_duty)

    def test_pause_finalises_at_idle_boundary_and_starts_keep_warm(self):
        self.set_pending_shot()
        self.plugin.queue_paused = True
        self.plugin.queue_pause_started = 12.0
        self.plugin._start_keep_warm = mock.Mock()

        with mock.patch.object(
            thermalization_plugin.time, 'monotonic', return_value=20.0
        ):
            self.plugin._begin_keep_warm_after_queue_idle()

        self.assertAlmostEqual(self.plugin.history.mean, 0.25)
        self.plugin._start_keep_warm.assert_called_once_with(20.0)

    def test_empty_queue_settle_requires_a_pending_completed_shot(self):
        self.plugin.queue_empty = True
        self.plugin._begin_keep_warm_after_queue_idle = mock.Mock()
        self.plugin._queue_empty_settled()
        self.plugin._begin_keep_warm_after_queue_idle.assert_not_called()

        self.set_pending_shot()
        self.plugin._queue_empty_settled()
        self.plugin._begin_keep_warm_after_queue_idle.assert_called_once_with()

    def test_repeat_reinsert_cancels_empty_queue_state(self):
        model = mock.Mock()
        queue = types.SimpleNamespace(_model=model)
        self.plugin.BLACS = {'experiment_queue': queue}
        self.plugin._schedule_keep_warm_for_empty_queue = mock.Mock()
        self.set_pending_shot()

        model.rowCount.return_value = 0
        with mock.patch.object(
            thermalization_plugin.time, 'monotonic', return_value=12.0
        ):
            self.plugin._queue_model_changed()
        self.assertTrue(self.plugin.queue_empty)
        self.assertEqual(self.plugin.queue_empty_started, 12.0)
        self.plugin._schedule_keep_warm_for_empty_queue.assert_called_once_with()

        model.rowCount.return_value = 1
        self.plugin._queue_model_changed()
        self.assertFalse(self.plugin.queue_empty)
        self.assertIsNone(self.plugin.queue_empty_started)

    def test_idle_timeout_finalises_and_starts_keep_warm(self):
        self.set_pending_shot(final_active=True)
        self.plugin._queue_interruption_reason = mock.Mock(return_value=None)
        self.plugin._has_device_error = mock.Mock(return_value=False)
        self.plugin._start_keep_warm = mock.Mock()

        with mock.patch.object(
            thermalization_plugin.time, 'monotonic', return_value=41.0
        ):
            self.plugin._on_timer_tick()

        self.assertAlmostEqual(self.plugin.history.mean, 32.0 / 33.0)
        self.plugin._start_keep_warm.assert_called_once_with(41.0)

    def test_keep_warm_edges_use_precise_epoch_based_scheduling(self):
        self.plugin.history.append_seconds(2, 5)
        self.plugin.keep_warm_timer = mock.Mock()
        self.plugin._set_target_active = mock.Mock()

        with mock.patch.object(
            thermalization_plugin.time, 'monotonic', return_value=100.0
        ):
            self.plugin._start_keep_warm(100.0)
        self.plugin._set_target_active.assert_called_once_with(True)
        self.plugin.keep_warm_timer.start.assert_called_with(4000)

        # A callback arriving 200 ms late is corrected against the original
        # epoch, rather than shifting all following transitions by 200 ms.
        self.plugin.keep_warm_timer.start.reset_mock()
        with mock.patch.object(
            thermalization_plugin.time, 'monotonic', return_value=104.2
        ):
            self.plugin._on_keep_warm_timer()
        self.plugin._set_target_active.assert_called_with(False)
        interval_ms = self.plugin.keep_warm_timer.start.call_args[0][0]
        self.assertIn(interval_ms, (5800, 5801))

        self.plugin.keep_warm_timer.start.reset_mock()
        with mock.patch.object(
            thermalization_plugin.time, 'monotonic', return_value=111.0
        ):
            self.plugin._on_keep_warm_timer()
        self.plugin._set_target_active.assert_called_with(True)
        self.plugin.keep_warm_timer.start.assert_called_with(3000)

    def test_constant_keep_warm_level_does_not_arm_edge_timer(self):
        self.plugin.history.append_seconds(0, 1)
        self.plugin.keep_warm_timer = mock.Mock()
        self.plugin._set_target_active = mock.Mock()
        with mock.patch.object(
            thermalization_plugin.time, 'monotonic', return_value=100.0
        ):
            self.plugin._start_keep_warm(100.0)
        self.plugin._set_target_active.assert_called_once_with(False)
        self.plugin.keep_warm_timer.stop.assert_called_once_with()
        self.plugin.keep_warm_timer.start.assert_not_called()

    def test_routine_pause_holds_output_and_stops_edge_timer(self):
        self.plugin.history.append_seconds(2, 5)
        self.plugin.keep_warm_timer = mock.Mock()
        self.plugin.empty_queue_timer = mock.Mock()
        self.plugin._set_target_active = mock.Mock()

        with mock.patch.object(
            thermalization_plugin.time, 'monotonic', return_value=100.0
        ):
            self.plugin._start_keep_warm(100.0)
        self.plugin._set_target_active.reset_mock()
        self.plugin.keep_warm_timer.reset_mock()

        self.plugin._routine_pause_toggled(True)
        self.plugin._on_keep_warm_timer()
        self.plugin._on_timer_tick()

        self.assertTrue(self.plugin.routine_paused)
        self.assertTrue(self.plugin.keep_warm_active)
        self.assertEqual(self.plugin.state, 'Paused')
        self.plugin.empty_queue_timer.stop.assert_called_once_with()
        self.plugin.keep_warm_timer.stop.assert_called_once_with()
        self.plugin._set_target_active.assert_not_called()

    def test_routine_resume_continues_keep_warm_switching(self):
        self.plugin.history.append_seconds(2, 5)
        self.plugin.keep_warm_timer = mock.Mock()
        self.plugin._set_target_active = mock.Mock()
        self.plugin.keep_warm_active = True
        self.plugin.keep_warm_started = 100.0
        self.plugin.keep_warm_output_active = True
        self.plugin.routine_paused = True

        with mock.patch.object(
            thermalization_plugin.time, 'monotonic', return_value=104.0
        ):
            self.plugin._routine_pause_toggled(False)

        self.assertFalse(self.plugin.routine_paused)
        self.plugin._set_target_active.assert_called_once_with(False)
        self.plugin.keep_warm_timer.start.assert_called_once_with(6000)

    def test_routine_resume_honours_queue_idle_boundary(self):
        self.set_pending_shot()
        self.plugin.routine_paused = True
        self.plugin.queue_empty = True
        self.plugin.queue_empty_started = 12.0
        self.plugin._start_keep_warm = mock.Mock()

        with mock.patch.object(
            thermalization_plugin.time, 'monotonic', return_value=20.0
        ):
            self.plugin._routine_pause_toggled(False)

        self.assertAlmostEqual(self.plugin.history.mean, 0.25)
        self.plugin._start_keep_warm.assert_called_once_with(20.0)

    def test_interruption_waits_for_manual_mode_then_starts_keep_warm(self):
        self.plugin.current_shot = {'path': 'interrupted.h5', 'ignore': False}
        self.plugin.history.append_seconds(1, 2)
        target_tab = types.SimpleNamespace(mode='buffered')
        self.plugin._target_tab = mock.Mock(return_value=target_tab)
        self.plugin._start_keep_warm = mock.Mock()

        with mock.patch.object(
            thermalization_plugin.time, 'monotonic', return_value=5.0
        ):
            self.plugin._begin_keep_warm_after_interruption('Experiment aborted')

        self.assertIsNone(self.plugin.current_shot)
        self.plugin._try_keep_warm_after_interruption(5.2)
        self.plugin._start_keep_warm.assert_not_called()
        self.assertEqual(self.plugin.interruption_reason, 'Experiment aborted')

        target_tab.mode = thermalization_plugin.MODE_MANUAL
        self.plugin._try_keep_warm_after_interruption(5.3)
        self.plugin._start_keep_warm.assert_called_once_with(5.3)
        self.assertIsNone(self.plugin.interruption_reason)
        self.assertIsNone(self.plugin.interruption_ready_at)

    def test_queue_interruption_statuses_are_classified(self):
        queue = mock.Mock()
        self.plugin.BLACS = {'experiment_queue': queue}
        cases = {
            'Device restarted during run': 'Device restarted',
            'Aborted': 'Experiment aborted',
            'Programming timed out': 'Experiment timed out',
            'Error in queue manager': 'Experiment failed',
            'Run failed': 'Experiment failed',
            'Idle': None,
        }
        for status, expected in cases.items():
            with self.subTest(status=status):
                queue.get_status.return_value = status
                self.assertEqual(
                    self.plugin._queue_interruption_reason(), expected
                )

    def test_abort_button_requests_interruption(self):
        self.plugin._begin_keep_warm_after_interruption = mock.Mock()
        self.plugin._abort_requested()
        self.plugin._begin_keep_warm_after_interruption.assert_called_once_with(
            'Experiment aborted'
        )

    def test_device_tab_errors_are_detected(self):
        tabs = {
            'healthy': types.SimpleNamespace(error_message='', state='idle'),
            'failed': types.SimpleNamespace(error_message='failure', state='idle'),
        }
        owner = types.SimpleNamespace(tablist=tabs)
        self.plugin.BLACS = {
            'experiment_queue': types.SimpleNamespace(BLACS=owner)
        }
        self.assertTrue(self.plugin._has_device_error())

    def test_next_shot_stops_keep_warm_and_is_marked_ignored(self):
        self.plugin.keep_warm_active = True
        with mock.patch.object(
            thermalization_plugin.time, 'monotonic', return_value=20.0
        ):
            self.plugin.pre_transition_to_buffered('next.h5')

        self.assertFalse(self.plugin.keep_warm_active)
        self.assertTrue(self.plugin.current_shot['ignore'])
        self.assertEqual(self.plugin.current_shot['path'], 'next.h5')

    def test_shot_read_failure_discards_current_sample(self):
        self.plugin.current_shot = {'path': 'bad.h5', 'ignore': False}
        self.plugin._read_shot_duty = mock.Mock(side_effect=RuntimeError('bad data'))
        self.plugin._set_error = mock.Mock()
        self.plugin.shot_complete('bad.h5')
        self.assertIsNone(self.plugin.current_shot)
        self.plugin._set_error.assert_called_once()


class TestPluginShotData(unittest.TestCase):
    def setUp(self):
        self.plugin = thermalization_plugin.Plugin({})
        thermalization_plugin.properties.get.reset_mock()
        thermalization_plugin.properties.get.side_effect = None
        thermalization_plugin.h5py.File.reset_mock()

    def test_structured_do_table_does_not_require_port_metadata(self):
        table = FakeDataset(
            [None, None],
            names=('port1',),
            fields={'port1': [0, 1]},
        )
        h5_file = {
            'devices': {thermalization_plugin.THERMAL_DEVICE_NAME: {'DO': table}}
        }
        self.assertEqual(
            self.plugin._read_ttl_levels(h5_file), [False, True]
        )
        thermalization_plugin.properties.get.assert_not_called()

    def test_completed_h5_data_combines_trace_and_measured_waits(self):
        table = FakeDataset(
            [None, None, None],
            names=('port1',),
            fields={'port1': [1, 0, 1]},
        )
        waits = FakeStructuredRows(
            {'time': [3], 'duration': [4]}, ('time', 'duration')
        )
        device_name = thermalization_plugin.THERMAL_DEVICE_NAME
        h5_file = FakeH5File(
            {
                'devices': {device_name: {'DO': table}},
                'devices/%s/TIMES' % device_name: FakeArray([0, 2, 5]),
                'data/waits': waits,
            }
        )
        thermalization_plugin.h5py.File.return_value = h5_file

        self.assertEqual(
            self.plugin._read_shot_duty('shot.h5'), (2.0, 9.0, True)
        )
        thermalization_plugin.h5py.File.assert_called_with('shot.h5', 'r')

    def test_one_dimensional_do_uses_connection_port_offsets(self):
        table = FakeDataset([0, 0x100])
        h5_file = {
            'devices': {thermalization_plugin.THERMAL_DEVICE_NAME: {'DO': table}}
        }
        thermalization_plugin.properties.get.return_value = {
            'ports': OrderedDict(
                [
                    ('port0', {'supports_buffered': True, 'num_lines': 8}),
                    ('port1', {'supports_buffered': True, 'num_lines': 8}),
                ]
            )
        }
        self.assertEqual(
            self.plugin._read_ttl_levels(h5_file), [False, True]
        )

    def test_two_dimensional_do_uses_configured_port_column(self):
        table = FakeDataset([[0, 0], [0, 1]], ndim=2)
        h5_file = {
            'devices': {thermalization_plugin.THERMAL_DEVICE_NAME: {'DO': table}}
        }
        self.assertEqual(
            self.plugin._read_ttl_levels(h5_file), [False, True]
        )
        thermalization_plugin.properties.get.assert_not_called()

    def test_two_dimensional_port_override_must_be_a_non_negative_integer(self):
        for index in (-1, 1.5):
            with self.subTest(index=index):
                with mock.patch.object(
                    thermalization_plugin, 'THERMAL_DO_PORT_INDEX', index
                ):
                    with self.assertRaisesRegex(ValueError, 'must be'):
                        thermalization_plugin._port_index('port1')

    def test_missing_and_empty_do_tables_have_clear_errors(self):
        with self.assertRaisesRegex(RuntimeError, 'no DO table'):
            self.plugin._read_ttl_levels({'devices': {}})

        empty = FakeDataset([], names=('port1',), fields={'port1': []})
        h5_file = {
            'devices': {thermalization_plugin.THERMAL_DEVICE_NAME: {'DO': empty}}
        }
        with self.assertRaisesRegex(RuntimeError, 'is empty'):
            self.plugin._read_ttl_levels(h5_file)

    def test_clock_data_must_exist_and_be_one_dimensional(self):
        with self.assertRaisesRegex(RuntimeError, 'no direct clock-times'):
            self.plugin._read_clock_times({}, 2)

        path = 'devices/%s/TIMES' % thermalization_plugin.THERMAL_DEVICE_NAME
        with self.assertRaisesRegex(RuntimeError, 'one-dimensional'):
            self.plugin._read_clock_times(
                {path: FakeArray([[0], [1]], ndim=2)}, 2
            )
        with self.assertRaisesRegex(RuntimeError, '4 samples for 2 DO rows'):
            self.plugin._read_clock_times({path: FakeArray([0, 1, 2, 3])}, 2)

    def test_clock_data_accepts_samples_or_interval_boundaries(self):
        path = 'devices/%s/TIMES' % thermalization_plugin.THERMAL_DEVICE_NAME
        samples = self.plugin._read_clock_times(
            {path: FakeArray([0, 1])}, 2
        )
        boundaries = self.plugin._read_clock_times(
            {path: FakeArray([0, 1, 2])}, 2
        )
        self.assertEqual(samples.data, [0, 1])
        self.assertEqual(boundaries.data, [0, 1, 2])

    def test_legacy_packed_layout_rejects_out_of_range_line(self):
        thermalization_plugin.properties.get.side_effect = KeyError('ports')
        with self.assertRaisesRegex(RuntimeError, 'outside legacy'):
            self.plugin._packed_bit_index({}, 'port1', 8)

    def test_missing_port_metadata_is_reported_for_current_files(self):
        thermalization_plugin.properties.get.return_value = {
            'ports': OrderedDict(
                [('port0', {'supports_buffered': True, 'num_lines': 8})]
            )
        }
        with self.assertRaisesRegex(RuntimeError, 'not a buffered NI port'):
            self.plugin._packed_bit_index({}, 'port1', 0)

    def test_malformed_port_metadata_is_reported(self):
        thermalization_plugin.properties.get.return_value = {
            'ports': {'port1': {'supports_buffered': True}}
        }
        with self.assertRaisesRegex(RuntimeError, 'metadata.*malformed'):
            self.plugin._packed_bit_index({}, 'port1', 0)

    def test_wait_data_requires_time_and_duration_fields(self):
        waits = FakeStructuredRows({'time': [1]}, ('time',))
        with self.assertRaisesRegex(RuntimeError, 'lacks time or duration'):
            self.plugin._read_waits({'data/waits': waits})

        waits = FakeStructuredRows(
            {'time': [1, 2], 'duration': [3, 4]}, ('time', 'duration')
        )
        self.assertEqual(
            self.plugin._read_waits({'data/waits': waits}),
            ([1, 2], [3, 4]),
        )


if __name__ == '__main__':
    unittest.main()
