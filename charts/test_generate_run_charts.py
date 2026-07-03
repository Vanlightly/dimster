import unittest
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
import generate_run_charts


class GenerateRunChartsTest(unittest.TestCase):
    def test_build_html_allows_null_top_level_config_blocks(self):
        """Interrupted runs can have null config blocks but still chart completed tests."""
        data = {
            "mode": "run",
            "metadata": {
                "name": "stopped run",
                "kafkaConfigName": None,
                "environment": "localBeefy",
                "system": "Kafka",
                "beginTime": "2026-07-02-16-09-41",
                "endTime": "2026-07-02-16-15-00",
            },
            "baseKafkaConfig": None,
            "runConfig": {
                "testDurationMinutes": 3,
                "warmupDurationMinutes": 2,
                "sampleRateMillis": 10000,
            },
            "availabilityConfig": None,
            "scenarioResults": [
                {
                    "name": "partial",
                    "tests": [
                        {
                            "label": "1K/s",
                            "kafkaConfig": {"replicationFactor": 3},
                            "workloadConfig": {"messageSize": 500},
                            "results": {
                                "publishRate": [1000.0],
                                "consumeRate": [1000.0],
                            },
                        }
                    ],
                }
            ],
        }

        chart_configs, chart_sections = generate_run_charts.build_chart_configs(data)
        main_configs, main_sections = generate_run_charts.filter_for_mode(
            chart_configs, chart_sections, "main"
        )
        html = generate_run_charts.build_html(data, main_configs, main_sections)

        self.assertIn("stopped run", html)
        self.assertIn("localBeefy", html)
        self.assertNotIn(">None<", html)


if __name__ == "__main__":
    unittest.main()
