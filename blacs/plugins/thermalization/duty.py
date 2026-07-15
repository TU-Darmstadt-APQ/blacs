"""Small, dependency-free helpers for thermalization duty accounting."""

from bisect import bisect_right
from collections import deque
from math import isfinite
from operator import index as integer_index


def _duty_from_boundaries(
    boundaries, values, active_high, monotonic_error, elapsed_error
):
    """Return duty seconds for values with an explicit boundary on each side."""
    active_time = 0.0
    total_time = 0.0
    previous_time = float(boundaries[0])
    if not isfinite(previous_time):
        raise ValueError('timestamps must be finite')
    for time, value in zip(boundaries[1:], values):
        time = float(time)
        if not isfinite(time):
            raise ValueError('timestamps must be finite')
        duration = time - previous_time
        if duration < 0:
            raise ValueError(monotonic_error)
        total_time += duration
        if bool(value) == active_high:
            active_time += duration
        previous_time = time

    if total_time <= 0:
        raise ValueError(elapsed_error)
    return active_time, total_time


def duty_from_trace(times, values, active_high=True):
    """Return ``(active_time, total_time)`` for a piecewise-constant trace.

    Each value applies from its timestamp up to the following timestamp.
    """
    if len(times) != len(values):
        raise ValueError('times and values must have the same length')
    if len(times) < 2:
        raise ValueError('a trace needs at least two samples')

    return _duty_from_boundaries(
        times,
        values[:-1],
        active_high,
        'trace timestamps must be monotonic',
        'trace has no elapsed time',
    )


def duty_from_intervals(boundaries, values, active_high=True):
    """Return duty seconds where every value has an explicit time interval."""
    if len(boundaries) != len(values) + 1:
        raise ValueError('interval boundaries must have one more item than values')

    return _duty_from_boundaries(
        boundaries,
        values,
        active_high,
        'interval boundaries must be monotonic',
        'intervals have no elapsed time',
    )


def packed_ttl_levels(packed_values, bit_index):
    """Unpack one TTL bit from a sequence of packed integer words."""
    try:
        bit_index = integer_index(bit_index)
    except TypeError:
        raise ValueError('bit index must be an integer')
    if bit_index < 0:
        raise ValueError('bit index must be non-negative')
    return [bool(int(value) & (1 << bit_index)) for value in packed_values]


def wait_duty_seconds(times, values, wait_times, wait_durations, active_high=True):
    """Return duty seconds contributed by measured waits in a shot timeline."""
    if len(times) != len(values) or len(times) == 0:
        raise ValueError('wait accounting requires one time for each value')
    if len(wait_times) != len(wait_durations):
        raise ValueError('wait times and durations must have the same length')

    sample_times = [float(time) for time in times]
    if not all(isfinite(time) for time in sample_times):
        raise ValueError('trace timestamps must be finite')
    if any(
        later < earlier
        for earlier, later in zip(sample_times, sample_times[1:])
    ):
        raise ValueError('trace timestamps must be monotonic')

    active_time = 0.0
    total_time = 0.0
    for wait_time, duration in zip(wait_times, wait_durations):
        wait_time = float(wait_time)
        if not isfinite(wait_time):
            raise ValueError('wait times must be finite')
        duration = float(duration)
        if not isfinite(duration):
            raise ValueError('wait duration must be finite')
        if duration < 0:
            raise ValueError('wait duration must not be negative')
        index = max(0, bisect_right(sample_times, wait_time) - 1)
        total_time += duration
        if bool(values[index]) == active_high:
            active_time += duration
    return active_time, total_time


class DutyHistory(object):
    """A bounded arithmetic (not time-weighted) history of shot duties."""

    def __init__(self, size):
        try:
            size = integer_index(size)
        except TypeError:
            raise ValueError('history size must be an integer')
        if size <= 0:
            raise ValueError('history size must be positive')
        self._values = deque(maxlen=size)

    def append_seconds(self, active_time, total_time):
        active_time = float(active_time)
        total_time = float(total_time)
        if not isfinite(active_time) or not isfinite(total_time):
            raise ValueError('sample times must be finite')
        if total_time <= 0:
            raise ValueError('sample duration must be positive')
        duty = active_time / total_time
        if not 0.0 <= duty <= 1.0:
            raise ValueError('active time must be within the sample duration')
        self._values.append(duty)
        return duty

    @property
    def count(self):
        return len(self._values)

    @property
    def mean(self):
        if not self._values:
            return None
        return sum(self._values) / len(self._values)


def keep_warm_level(duty, elapsed, period):
    """Return the desired active state at ``elapsed`` seconds into a cycle."""
    duty = float(duty)
    elapsed = float(elapsed)
    period = float(period)
    if not all(isfinite(value) for value in (duty, elapsed, period)):
        raise ValueError('duty, elapsed, and period must be finite')
    if not 0.0 <= duty <= 1.0:
        raise ValueError('duty must be between zero and one')
    if period <= 0:
        raise ValueError('period must be positive')
    return (elapsed % period) < duty * period
