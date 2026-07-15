import unittest
import importlib.util
from pathlib import Path


MODULE_PATH = Path(__file__).parents[1] / 'blacs/plugins/thermalization/duty.py'
SPEC = importlib.util.spec_from_file_location('thermalization_duty', MODULE_PATH)
thermalization_duty = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(thermalization_duty)
DutyHistory = thermalization_duty.DutyHistory
duty_from_trace = thermalization_duty.duty_from_trace
duty_from_intervals = thermalization_duty.duty_from_intervals
keep_warm_level = thermalization_duty.keep_warm_level
packed_ttl_levels = thermalization_duty.packed_ttl_levels


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


class TestPackedTTLLevels(unittest.TestCase):
    def test_four_ports_are_combined_in_a_uint32(self):
        # port1/line0 is bit 8 when each port occupies one byte.
        self.assertEqual(packed_ttl_levels([0x00000000, 0x00000100, 0x00000001], 8),
                         [False, True, False])


if __name__ == '__main__':
    unittest.main()
