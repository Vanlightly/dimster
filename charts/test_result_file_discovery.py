import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(__file__))
import generate_correctness_charts
import generate_drain_backlog_charts
import generate_explore_charts
import generate_run_charts

# Each chart script's discovery function, keyed by the result mode it charts.
FINDERS = {
    "run": generate_run_charts.find_run_json_files,
    "explore": generate_explore_charts.find_explore_results,
    "drain-backlog": generate_drain_backlog_charts.find_drain_backlog_json_files,
    "correctness": generate_correctness_charts.find_correctness_json_files,
}


def _write(directory, name, data):
    with open(os.path.join(directory, name), "w") as fh:
        json.dump(data, fh)


class ResultFileDiscoveryTest(unittest.TestCase):
    """Run requests share a result's mode; only results should be charted."""

    def test_discovery_skips_typed_run_request(self):
        for mode, find in FINDERS.items():
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                _write(directory, "result.json", {"fileType": "dimster-result", "mode": mode})
                _write(directory, "run-request.json", {"fileType": "dimster-run-request", "mode": mode})

                found = find(directory)

                self.assertEqual(1, len(found))
                self.assertTrue(found[0][0].endswith("result.json"))

    def test_discovery_accepts_untyped_legacy_result(self):
        for mode, find in FINDERS.items():
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                _write(directory, "legacy.json", {"mode": mode})

                self.assertEqual(1, len(find(directory)))


if __name__ == "__main__":
    unittest.main()
