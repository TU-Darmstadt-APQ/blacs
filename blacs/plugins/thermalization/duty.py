"""Small, dependency-free helpers for thermalization duty accounting."""

from collections import deque


def duty_from_trace(times, values, active_high=True):
    """Return ``(active_time, total_time)`` for a piecewise-constant trace.

    Each value applies from its timestamp up to the following timestamp.
    """
    if len(times) != len(values):
        raise ValueError('times and values must have the same length')
    if len(times) < 2:
        raise ValueError('a trace needs at least two samples')

    active_time = 0.0
    total_time = 0.0
    previous_time = float(times[0])
    for time, value in zip(times[1:], values[:-1]):
        time = float(time)
        duration = time - previous_time
        if duration < 0:
            raise ValueError('trace timestamps must be monotonic')
        total_time += duration
        if bool(value) == active_high:
            active_time += duration
        previous_time = time

    if total_time <= 0:
        raise ValueError('trace has no elapsed time')
    return active_time, total_time


def duty_from_intervals(boundaries, values, active_high=True):
    """Return duty seconds where every value has an explicit time interval."""
    if len(boundaries) != len(values) + 1:
        raise ValueError('interval boundaries must have one more item than values')

    active_time = 0.0
    total_time = 0.0
    previous_time = float(boundaries[0])
    for time, value in zip(boundaries[1:], values):
        time = float(time)
        duration = time - previous_time
        if duration < 0:
            raise ValueError('interval boundaries must be monotonic')
        total_time += duration
        if bool(value) == active_high:
            active_time += duration
        previous_time = time

    if total_time <= 0:
        raise ValueError('intervals have no elapsed time')
    return active_time, total_time


class DutyHistory(object):
    """A bounded arithmetic (not time-weighted) history of shot duties."""

    def __init__(self, size):
        self._values = deque(maxlen=size)

    def append_seconds(self, active_time, total_time):
        if total_time <= 0:
            raise ValueError('sample duration must be positive')
        duty = float(active_time) / float(total_time)
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
    if not 0.0 <= duty <= 1.0:
        raise ValueError('duty must be between zero and one')
    if period <= 0:
        raise ValueError('period must be positive')
    return (elapsed % period) < duty * period
