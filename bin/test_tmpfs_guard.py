import importlib.util
import os
import unittest

spec = importlib.util.spec_from_file_location('tmpfs_guard', os.path.join(os.path.dirname(os.path.abspath(__file__)), 'tmpfs-guard.py'))
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)
GIB, MIB = mod.GIB, mod.MIB


def run(series, **kw):
    guard = mod.Guard(10 * GIB, 12 * GIB, **kw)
    out = []
    for t, used in series:
        out.append(guard.feed(float(t), used))
    return out


def sawtooth(seconds, floor, peak, period=54, start=0):
    # fills from floor to peak over one period, then drops back to floor (a roll that reclaims)
    return [(start + t, floor + (peak - floor) * (t % period) / period) for t in range(seconds)]


class TmpfsGuardTest(unittest.TestCase):
    def test_stable_sawtooth_never_aborts(self):
        result = run(sawtooth(600, 3 * GIB, 8 * GIB))
        self.assertEqual({s for s, _ in result}, {'ok'})

    def test_warn_then_abort_on_absolute_thresholds(self):
        result = run([(0, GIB), (1, 11 * GIB), (2, 13 * GIB)], warmup_s=1000)
        self.assertEqual([s for s, _ in result], ['ok', 'warn', 'abort'])

    def test_rising_floor_aborts_before_the_absolute_threshold(self):
        # the floor climbs 20 MiB per second after a stable start: 600 MiB in 30 s
        series = sawtooth(200, 3 * GIB, 6 * GIB)
        series += [(200 + t, 3 * GIB + 20 * MIB * t + (3 * GIB) * ((t % 54) / 54)) for t in range(0, 120)]
        result = run(series)
        aborts = [i for i, (s, _) in enumerate(result) if s == 'abort']
        self.assertTrue(aborts, 'a rising floor must abort')
        self.assertIn('floor rose', result[aborts[0]][1])
        self.assertLess(max(u for _, u in series[:aborts[0] + 1]), 12 * GIB)

    def test_slow_drift_below_the_rise_threshold_is_ignored(self):
        series = [(t, 3 * GIB + 1 * MIB * t + (3 * GIB) * ((t % 54) / 54)) for t in range(600)]
        self.assertEqual({s for s, _ in run(series)}, {'ok'})

    def test_warmup_is_not_evaluated(self):
        series = [(t, 100 * MIB * t) for t in range(80)]    # filling fast at the start, below the absolute thresholds
        self.assertEqual({s for s, _ in run(series, warmup_s=90)}, {'ok'})

    def test_a_dip_in_the_floor_resets_the_rise(self):
        series = sawtooth(200, 3 * GIB, 6 * GIB)
        series += [(200 + t, (3 * GIB + 20 * MIB * t if t < 20 else 3 * GIB + 20 * MIB * 20 - 500 * MIB) + (2 * GIB) * ((t % 54) / 54))
                   for t in range(0, 40)]
        self.assertNotIn('abort', {s for s, _ in run(series)})


if __name__ == '__main__':
    unittest.main()
