"""Tests for compute_windows in dashboard-export-configmap.yaml.

The export script lives inline in a Kubernetes ConfigMap so it can run
unmodified inside a python:3-slim pod with no extra build step. To unit
test the Python without duplicating the source we extract the script body
from the YAML at test time, exec it in an isolated namespace, and call
the function under test from there.

Run with:  python3 test_compute_windows.py
"""

import io
import os
import sys
import textwrap
import unittest


REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
CONFIGMAP_PATH = os.path.join(REPO_ROOT, 'dashboard-export-configmap.yaml')


def load_export_script():
    """Pull the export.py body out of the ConfigMap and return it as a string.

    The ConfigMap stores it under ``data.export.py:`` with a literal block
    scalar (``|``), 4-space-indented relative to the ``data:`` mapping. We
    strip that uniform indent so the result is valid standalone Python.
    """
    with open(CONFIGMAP_PATH, 'r') as f:
        lines = f.readlines()

    # Find the "  export.py: |" line, then collect indented lines below it
    # until the indent drops back. This is good enough for our well-formed
    # ConfigMap and avoids dragging in PyYAML as a test dependency.
    start = None
    for i, line in enumerate(lines):
        if line.strip().startswith('export.py:') and line.rstrip().endswith('|'):
            start = i + 1
            break
    if start is None:
        raise RuntimeError('Could not locate export.py block in ConfigMap')

    body_lines = []
    for line in lines[start:]:
        # The block scalar's contents are indented 4 spaces under "  export.py: |".
        # An empty/whitespace-only line still belongs to the block.
        if line.strip() == '':
            body_lines.append('')
            continue
        if line.startswith('    '):
            body_lines.append(line[4:].rstrip('\n'))
        else:
            break
    return '\n'.join(body_lines)


def load_compute_windows():
    """Exec the script in an isolated namespace and return compute_windows.

    The script's ``main()`` is guarded by ``if __name__ == '__main__'``, so
    exec-ing the module body just defines functions without running anything.
    """
    script = load_export_script()
    ns = {'__name__': 'export_module'}
    exec(compile(script, '<dashboard-export>', 'exec'), ns)
    return ns['compute_windows']


class ComputeWindowsTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.compute_windows = staticmethod(load_compute_windows())

    # --- Single-window cases (duration <= threshold) ----------------------

    def test_short_campaign_returns_single_window(self):
        # 2h campaign with 3h target — no splitting needed.
        windows = self.compute_windows(0, 2 * 3600, 3.0, 10.0)
        self.assertEqual(len(windows), 1)
        w = windows[0]
        self.assertEqual(w['index'], 1)
        self.assertEqual(w['total'], 1)
        self.assertEqual(w['core_start'], 0)
        self.assertEqual(w['core_end'], 2 * 3600)
        self.assertEqual(w['start'], -600)  # buffer = 10 minutes
        self.assertEqual(w['end'], 2 * 3600 + 600)

    def test_window_hours_zero_disables_splitting(self):
        windows = self.compute_windows(0, 100 * 3600, 0, 10.0)
        self.assertEqual(len(windows), 1)

    # --- Even-split fallback (no scenarios) -------------------------------

    def test_long_campaign_no_scenarios_splits_evenly(self):
        # 7h campaign with 3h target should split into 3 ~2.33h windows.
        end = 7 * 3600
        windows = self.compute_windows(0, end, 3.0, 10.0)
        self.assertEqual(len(windows), 3)
        # First window starts at campaign start; last window ends at exact end
        self.assertEqual(windows[0]['core_start'], 0)
        self.assertEqual(windows[-1]['core_end'], end)
        # No scenario context provided — names should be empty
        for w in windows:
            self.assertEqual(w['scenario_names'], [])
        # Cores abut: window N's core_end == window N+1's core_start
        for i in range(len(windows) - 1):
            self.assertEqual(windows[i]['core_end'], windows[i + 1]['core_start'])

    # --- Scenario-aware splitting -----------------------------------------

    def test_scenarios_short_enough_for_one_window(self):
        # Three 30-minute scenarios — fits in a 3h target, single window.
        scenarios = [
            {'name': 'a', 'start': 0,    'end': 1800},
            {'name': 'b', 'start': 1900, 'end': 3700},
            {'name': 'c', 'start': 3800, 'end': 5600},
        ]
        # Total span 5600s ~ 1.55h, under threshold => single-window path
        # bypasses scenario logic but still records names.
        windows = self.compute_windows(0, 5600, 3.0, 10.0, scenarios=scenarios)
        self.assertEqual(len(windows), 1)
        self.assertEqual(windows[0]['scenario_names'], ['a', 'b', 'c'])

    def test_long_campaign_packs_scenarios_without_splitting_them(self):
        # Campaign: 6h, target 2h. Five 1h scenarios with brief gaps.
        # Greedy packing: scenarios 1+2 (2h), 3+4 (2h), 5 (1h).
        scenarios = []
        for i in range(5):
            s = i * 3600 + i * 60     # 1m gap between scenarios
            e = s + 3600
            scenarios.append({'name': f's{i}', 'start': s, 'end': e})
        end = scenarios[-1]['end'] + 60  # tiny tail
        windows = self.compute_windows(0, end, 2.0, 10.0, scenarios=scenarios)
        # Expect three bins: [s0,s1], [s2,s3], [s4]
        self.assertEqual(len(windows), 3)
        self.assertEqual(windows[0]['scenario_names'], ['s0', 's1'])
        self.assertEqual(windows[1]['scenario_names'], ['s2', 's3'])
        self.assertEqual(windows[2]['scenario_names'], ['s4'])

    def test_scenario_boundaries_align_at_midpoint_of_gap(self):
        # Two scenarios separated by a 60s gap; with target 1h and two 45m
        # scenarios we get one scenario per bin. The cut between bin 1 and
        # bin 2 should be at the midpoint of the gap (s0.end + 30s).
        scenarios = [
            {'name': 's0', 'start': 0,        'end': 2700},   # 0..45m
            {'name': 's1', 'start': 2760,     'end': 5460},   # 46..91m
        ]
        end = 5460 + 120
        windows = self.compute_windows(0, end, 1.0, 10.0, scenarios=scenarios)
        self.assertEqual(len(windows), 2)
        self.assertEqual(windows[0]['scenario_names'], ['s0'])
        self.assertEqual(windows[1]['scenario_names'], ['s1'])
        # Midpoint between scenarios = (2700 + 2760) / 2 = 2730
        self.assertEqual(windows[0]['core_end'], 2730)
        self.assertEqual(windows[1]['core_start'], 2730)
        # Edges pinned to campaign boundaries
        self.assertEqual(windows[0]['core_start'], 0)
        self.assertEqual(windows[1]['core_end'], end)

    def test_oversized_scenario_gets_its_own_window(self):
        # One scenario larger than target — must not loop, must not be split.
        # Co-location promise wins over file-size target.
        scenarios = [
            {'name': 'short_a', 'start': 0,    'end': 1800},   # 30m
            {'name': 'huge',    'start': 2000, 'end': 20000},  # 5h
            {'name': 'short_b', 'start': 20100,'end': 21900},  # 30m
        ]
        end = 22000
        windows = self.compute_windows(0, end, 1.0, 10.0, scenarios=scenarios)
        # Greedy: bin1=[short_a] (full at 30m, adding huge would push to >1h),
        #         bin2=[huge] (alone — bigger than target, gets its own bin),
        #         bin3=[short_b]
        self.assertEqual(len(windows), 3)
        self.assertEqual(windows[0]['scenario_names'], ['short_a'])
        self.assertEqual(windows[1]['scenario_names'], ['huge'])
        self.assertEqual(windows[2]['scenario_names'], ['short_b'])

    def test_unsorted_scenarios_are_handled(self):
        # Defensive: caller may not pre-sort. Algorithm sorts internally.
        scenarios = [
            {'name': 'b', 'start': 4000, 'end': 8000},
            {'name': 'a', 'start': 0,    'end': 4000},
        ]
        windows = self.compute_windows(0, 8500, 1.0, 10.0, scenarios=scenarios)
        # Sorted, two scenarios, target 1h => two bins
        self.assertEqual(len(windows), 2)
        self.assertEqual(windows[0]['scenario_names'], ['a'])
        self.assertEqual(windows[1]['scenario_names'], ['b'])

    def test_buffer_extends_core_window(self):
        scenarios = [
            {'name': 's0', 'start': 0,    'end': 4000},
            {'name': 's1', 'start': 4500, 'end': 8500},
        ]
        windows = self.compute_windows(0, 9000, 1.0, 10.0, scenarios=scenarios)
        for w in windows:
            self.assertEqual(w['start'], w['core_start'] - 600)
            self.assertEqual(w['end'], w['core_end'] + 600)

    def test_each_scenario_lives_in_exactly_one_window(self):
        # Property check: every scenario name appears in exactly one window's
        # name list. This is the core invariant the user asked for.
        scenarios = [
            {'name': f's{i}', 'start': i * 1800, 'end': (i + 1) * 1800 - 60}
            for i in range(8)
        ]
        end = scenarios[-1]['end'] + 100
        windows = self.compute_windows(0, end, 1.0, 10.0, scenarios=scenarios)
        seen = []
        for w in windows:
            seen.extend(w['scenario_names'])
        self.assertEqual(sorted(seen), sorted(s['name'] for s in scenarios))


if __name__ == '__main__':
    unittest.main()
