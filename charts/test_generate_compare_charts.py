import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(__file__))
import generate_compare_charts


def _run_result(name):
    """Build the smallest run result shape the compare chart builder needs."""
    return {
        "mode": "run",
        "metadata": {
            "name": name,
            "system": "Kafka",
            "environment": "local",
            "kafkaConfigName": "safe",
            "beginTime": "2026-07-17T12:00:00Z",
            "endTime": "2026-07-17T12:10:00Z",
        },
        "runConfig": {
            "sampleRateMillis": 10000,
            "testDurationMinutes": 1,
            "warmupDurationMinutes": 0,
        },
        "baseKafkaConfig": {"replicationFactor": 1},
        "scenarioResults": [
            {
                "name": "default",
                "tests": [
                    {
                        "label": "baseline",
                        "workloadConfig": {
                            "topics": 1,
                            "partitionsPerTopic": 1,
                            "messageSize": 1000,
                            "producersPerTopic": 1,
                            "producerRate": 1000,
                            "consumerGroupsPerTopic": 1,
                            "consumersPerGroup": 1,
                            "messageDistributor": "NO_KEY",
                            "consumerType": "CONSUMER_GROUP",
                        },
                        "results": {
                            "publishRate": [1000.0, 1001.0],
                            "consumeRate": [998.0, 1002.0],
                            "publishLatency99pct": [2.0, 2.1],
                            "endToEndLatency99pct": [5.0, 5.2],
                            "publishDelayLatency99pct": [1.0, 1.1],
                            "aggregatedPublishLatencyQuantiles": {"0.5": 1.0, "0.99": 2.0},
                            "aggregatedEndToEndLatencyQuantiles": {"0.5": 3.0, "0.99": 5.0},
                        },
                    }
                ],
            }
        ],
    }


class GenerateCompareChartsTest(unittest.TestCase):
    def test_build_compare_charts_accepts_more_than_four_entries(self):
        entries = [
            (f"run-{i}", f"/tmp/run-{i}.json", _run_result(f"run-{i}"))
            for i in range(5)
        ]

        generate_compare_charts.validate_compare(entries)
        chart_configs, chart_sections = generate_compare_charts.build_compare_chart_configs(entries)
        html = generate_compare_charts.build_compare_html(entries, chart_configs, chart_sections)

        self.assertGreater(len(chart_configs), 0)
        self.assertIn("run-4", html)
        self.assertIn("Dimster Compare", html)


if __name__ == "__main__":
    unittest.main()
