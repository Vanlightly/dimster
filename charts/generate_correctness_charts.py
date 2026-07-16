#!/usr/bin/env python3
"""
Generate HTML charts from Dimster correctness result JSON files.

Usage:
    python3 generate_correctness_charts.py --results <dir> --output <dir>

Scans --results dir recursively for JSON files where mode == "correctness",
generates one HTML file per result using Chart.js for visualization.
"""

import argparse
import json
import math
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from generate_run_charts import metadata_annotation_html


COLORS = [
    "#4e79a7",  # steel blue
    "#e15759",  # coral
    "#59a14f",  # green
    "#f28e2b",  # amber
    "#9c755f",  # taupe
    "#76b7b2",  # teal
]

COLORS_ALPHA = [
    "rgba(78, 121, 167, 0.15)",
    "rgba(225, 87, 89, 0.15)",
    "rgba(89, 161, 79, 0.15)",
    "rgba(242, 142, 43, 0.15)",
    "rgba(156, 117, 95, 0.15)",
    "rgba(118, 183, 178, 0.15)",
]


def find_correctness_json_files(results_dir):
    """Recursively find JSON files with mode == 'correctness'."""
    found = []
    for root, _dirs, files in os.walk(results_dir):
        for f in files:
            if not f.endswith(".json"):
                continue
            path = os.path.join(root, f)
            try:
                with open(path, "r") as fh:
                    data = json.load(fh)
                if data.get("mode") == "correctness":
                    found.append((path, data))
            except json.JSONDecodeError as e:
                print(f"Warning: skipping {path}: invalid JSON: {e}", file=sys.stderr)
            except IOError:
                continue
    return found


def avg(lst):
    if not lst:
        return 0
    return sum(lst) / len(lst)


def time_labels(arr_len, sample_rate_ms):
    step = sample_rate_ms / 1000
    return [round(i * step) for i in range(arr_len)]


def line_dataset(label, data, color_idx):
    c = COLORS[color_idx % len(COLORS)]
    ca = COLORS_ALPHA[color_idx % len(COLORS_ALPHA)]
    return {
        "label": label,
        "data": data,
        "borderColor": c,
        "backgroundColor": ca,
        "borderWidth": 2,
        "pointRadius": 0,
        "pointHoverRadius": 4,
        "tension": 0.2,
        "fill": False,
    }


def bar_options(title, y_label):
    return {
        "responsive": True,
        "maintainAspectRatio": True,
        "plugins": {
            "title": {"display": True, "text": title, "font": {"size": 14, "weight": "600"}},
            "legend": {"position": "top", "labels": {"usePointStyle": True, "padding": 16}},
            "tooltip": {"enabled": True},
        },
        "scales": {
            "y": {
                "beginAtZero": True,
                "title": {"display": True, "text": y_label},
                "grid": {"color": "#e0e0e0"},
            },
            "x": {"grid": {"display": False}},
        },
    }


def line_options(title, y_label, x_label):
    return {
        "responsive": True,
        "maintainAspectRatio": True,
        "plugins": {
            "title": {"display": True, "text": title, "font": {"size": 14, "weight": "600"}},
            "legend": {"position": "top", "labels": {"usePointStyle": True, "padding": 16}},
            "tooltip": {"enabled": True, "mode": "index", "intersect": False},
        },
        "scales": {
            "y": {
                "beginAtZero": True,
                "title": {"display": True, "text": y_label},
                "grid": {"color": "#e0e0e0"},
            },
            "x": {
                "title": {"display": True, "text": x_label},
                "grid": {"color": "#f0f0f0"},
            },
        },
        "interaction": {"mode": "nearest", "axis": "x", "intersect": False},
    }


def percentile_to_x(p):
    if p >= 100.0:
        return 5.0
    return math.log10(100.0 / (100.0 - p))


def percentile_points(quantiles_map):
    points = []
    for pct_str, latency in sorted(quantiles_map.items(), key=lambda kv: float(kv[0])):
        pct = float(pct_str)
        if pct > 100.0:
            continue
        points.append({"x": round(percentile_to_x(pct), 4), "y": latency})
    return points


def percentile_dataset(label, points, color_idx):
    c = COLORS[color_idx % len(COLORS)]
    return {
        "label": label,
        "data": points,
        "borderColor": c,
        "backgroundColor": c,
        "borderWidth": 1.5,
        "pointRadius": 0,
        "pointHoverRadius": 5,
        "showLine": True,
        "tension": 0.3,
        "fill": False,
    }


PERCENTILE_TICKS = [
    (50, "p50"), (75, "p75"), (90, "p90"), (95, "p95"),
    (99, "p99"), (99.9, "p99.9"), (99.99, "p99.99"), (100, "max"),
]
PERCENTILE_TICK_VALUES = [round(percentile_to_x(p), 4) for p, _ in PERCENTILE_TICKS]
PERCENTILE_TICK_LABELS = [label for _, label in PERCENTILE_TICKS]


def percentile_options(title):
    return {
        "responsive": True,
        "maintainAspectRatio": True,
        "plugins": {
            "title": {"display": True, "text": title, "font": {"size": 14, "weight": "600"}},
            "legend": {"position": "top", "labels": {"usePointStyle": True, "padding": 16}},
            "tooltip": {
                "enabled": True,
                "callbacks": {
                    "__percentile_tooltip": True,
                },
            },
        },
        "scales": {
            "x": {
                "type": "linear",
                "title": {"display": True, "text": "Percentile"},
                "min": PERCENTILE_TICK_VALUES[0],
                "max": PERCENTILE_TICK_VALUES[-1],
                "ticks": {
                    "__percentile_ticks": True,
                },
                "grid": {"color": "#f0f0f0"},
            },
            "y": {
                "beginAtZero": True,
                "title": {"display": True, "text": "Latency (ms)"},
                "grid": {"color": "#e0e0e0"},
            },
        },
        "interaction": {"mode": "nearest", "intersect": False},
    }


def build_chart_configs(data):
    """Build all chart configs for a correctness result."""
    config = data.get("config", {})
    correctness_config = config.get("correctnessConfig", {})
    sample_rate_ms = correctness_config.get("sampleRateMillis", 10000)
    scenarios = data.get("scenarioResults", [])

    chart_configs = []
    chart_counts = {}

    for s_idx, scenario in enumerate(scenarios):
        tests = scenario.get("tests", [])
        if not tests:
            continue

        labels = [t.get("label", f"test-{i}") for i, t in enumerate(tests)]
        chart_idx = 0

        # --- 1. Throughput (grouped bar) ---
        avg_pub = [round(avg(t.get("performance", {}).get("publishRate", [])), 1) for t in tests]
        avg_con = [round(avg(t.get("performance", {}).get("consumeRate", [])), 1) for t in tests]
        chart_configs.append({
            "id": f"chart-{s_idx}-{chart_idx}",
            "config": {
                "type": "bar",
                "data": {
                    "labels": labels,
                    "datasets": [
                        {"label": "Publish", "data": avg_pub, "backgroundColor": COLORS[0]},
                        {"label": "Consume", "data": avg_con, "backgroundColor": COLORS[1]},
                    ],
                },
                "options": bar_options("Throughput (msg/s)", "msg/s"),
            },
        })
        chart_idx += 1

        # --- 2. Aggregated Latency - Publish ---
        pctiles = ["p50", "p95", "p99", "p99.9", "max"]
        pctile_keys = [
            "aggregatedPublishLatency50pct",
            "aggregatedPublishLatency95pct",
            "aggregatedPublishLatency99pct",
            "aggregatedPublishLatency999pct",
            "aggregatedPublishLatencyMax",
        ]
        datasets_pub_lat = []
        for pi, (pname, pkey) in enumerate(zip(pctiles, pctile_keys)):
            datasets_pub_lat.append({
                "label": pname,
                "data": [t.get("performance", {}).get(pkey, 0) for t in tests],
                "backgroundColor": COLORS[pi % len(COLORS)],
            })
        chart_configs.append({
            "id": f"chart-{s_idx}-{chart_idx}",
            "config": {
                "type": "bar",
                "data": {"labels": labels, "datasets": datasets_pub_lat},
                "options": bar_options("Aggregated Latency - Publish", "ms"),
            },
        })
        chart_idx += 1

        # --- 3. Aggregated Latency - E2E ---
        e2e_keys = [
            "aggregatedEndToEndLatency50pct",
            "aggregatedEndToEndLatency95pct",
            "aggregatedEndToEndLatency99pct",
            "aggregatedEndToEndLatency999pct",
            "aggregatedEndToEndLatencyMax",
        ]
        datasets_e2e_lat = []
        for pi, (pname, pkey) in enumerate(zip(pctiles, e2e_keys)):
            datasets_e2e_lat.append({
                "label": pname,
                "data": [t.get("performance", {}).get(pkey, 0) for t in tests],
                "backgroundColor": COLORS[pi % len(COLORS)],
            })
        chart_configs.append({
            "id": f"chart-{s_idx}-{chart_idx}",
            "config": {
                "type": "bar",
                "data": {"labels": labels, "datasets": datasets_e2e_lat},
                "options": bar_options("Aggregated Latency - E2E", "ms"),
            },
        })
        chart_idx += 1

        # --- 4. Publish Rate over time ---
        max_len = max((len(t.get("performance", {}).get("publishRate", [])) for t in tests), default=0)
        x_labels = time_labels(max_len, sample_rate_ms)
        datasets_pub_ts = []
        for ti, t in enumerate(tests):
            datasets_pub_ts.append(line_dataset(
                labels[ti], t.get("performance", {}).get("publishRate", []), ti
            ))
        chart_configs.append({
            "id": f"chart-{s_idx}-{chart_idx}",
            "zoomGroup": "time",
            "config": {
                "type": "line",
                "data": {"labels": x_labels, "datasets": datasets_pub_ts},
                "options": line_options("Publish Rate over Time", "msg/s", "Time (s)"),
            },
        })
        chart_idx += 1

        # --- 5. Consume Rate over time ---
        max_len = max((len(t.get("performance", {}).get("consumeRate", [])) for t in tests), default=0)
        x_labels = time_labels(max_len, sample_rate_ms)
        datasets_con_ts = []
        for ti, t in enumerate(tests):
            datasets_con_ts.append(line_dataset(
                labels[ti], t.get("performance", {}).get("consumeRate", []), ti
            ))
        chart_configs.append({
            "id": f"chart-{s_idx}-{chart_idx}",
            "zoomGroup": "time",
            "config": {
                "type": "line",
                "data": {"labels": x_labels, "datasets": datasets_con_ts},
                "options": line_options("Consume Rate over Time", "msg/s", "Time (s)"),
            },
        })
        chart_idx += 1

        # --- 6. Publish Latency p99 over time ---
        max_len = max((len(t.get("performance", {}).get("publishLatency99pct", [])) for t in tests), default=0)
        x_labels = time_labels(max_len, sample_rate_ms)
        datasets_plat = []
        for ti, t in enumerate(tests):
            datasets_plat.append(line_dataset(
                labels[ti], t.get("performance", {}).get("publishLatency99pct", []), ti
            ))
        chart_configs.append({
            "id": f"chart-{s_idx}-{chart_idx}",
            "zoomGroup": "time",
            "config": {
                "type": "line",
                "data": {"labels": x_labels, "datasets": datasets_plat},
                "options": line_options("Publish Latency p99 over Time", "ms", "Time (s)"),
            },
        })
        chart_idx += 1

        # --- 7. E2E Latency p99 over time ---
        max_len = max((len(t.get("performance", {}).get("endToEndLatency99pct", [])) for t in tests), default=0)
        x_labels = time_labels(max_len, sample_rate_ms)
        datasets_elat = []
        for ti, t in enumerate(tests):
            datasets_elat.append(line_dataset(
                labels[ti], t.get("performance", {}).get("endToEndLatency99pct", []), ti
            ))
        chart_configs.append({
            "id": f"chart-{s_idx}-{chart_idx}",
            "zoomGroup": "time",
            "config": {
                "type": "line",
                "data": {"labels": x_labels, "datasets": datasets_elat},
                "options": line_options("E2E Latency p99 over Time", "ms", "Time (s)"),
            },
        })
        chart_idx += 1

        # --- 8. Publish Delay p99 over time ---
        max_len = max((len(t.get("performance", {}).get("publishDelayLatency99pct", [])) for t in tests), default=0)
        x_labels = time_labels(max_len, sample_rate_ms)
        datasets_pdlat = []
        for ti, t in enumerate(tests):
            datasets_pdlat.append(line_dataset(
                labels[ti], t.get("performance", {}).get("publishDelayLatency99pct", []), ti
            ))
        chart_configs.append({
            "id": f"chart-{s_idx}-{chart_idx}",
            "zoomGroup": "time",
            "config": {
                "type": "line",
                "data": {"labels": x_labels, "datasets": datasets_pdlat},
                "options": line_options("Publish Delay p99 over Time", "ms", "Time (s)"),
            },
        })
        chart_idx += 1

        # --- 9. Publish Latency Percentile Distribution ---
        pub_quantile_datasets = []
        for ti, t in enumerate(tests):
            quantiles = t.get("performance", {}).get("aggregatedPublishLatencyQuantiles", {})
            if quantiles:
                points = percentile_points(quantiles)
                pub_quantile_datasets.append(percentile_dataset(labels[ti], points, ti))
        if pub_quantile_datasets:
            chart_configs.append({
                "id": f"chart-{s_idx}-{chart_idx}",
                "zoomGroup": "percentile",
                "config": {
                    "type": "scatter",
                    "data": {"datasets": pub_quantile_datasets},
                    "options": percentile_options("Publish Latency Percentile Distribution"),
                },
            })
            chart_idx += 1

        # --- 10. E2E Latency Percentile Distribution ---
        e2e_quantile_datasets = []
        for ti, t in enumerate(tests):
            quantiles = t.get("performance", {}).get("aggregatedEndToEndLatencyQuantiles", {})
            if quantiles:
                points = percentile_points(quantiles)
                e2e_quantile_datasets.append(percentile_dataset(labels[ti], points, ti))
        if e2e_quantile_datasets:
            chart_configs.append({
                "id": f"chart-{s_idx}-{chart_idx}",
                "zoomGroup": "percentile",
                "config": {
                    "type": "scatter",
                    "data": {"datasets": e2e_quantile_datasets},
                    "options": percentile_options("E2E Latency Percentile Distribution"),
                },
            })
            chart_idx += 1

        chart_counts[s_idx] = chart_idx

    return chart_configs, chart_counts


def build_sidebar_html(scenarios):
    items = []
    items.append('<div class="sidebar-label">Overview</div>')
    items.append('<a class="nav-link" href="#correctness-summary">Correctness Summary</a>')
    items.append('<a class="nav-link" href="#config-section">Configuration</a>')
    items.append('<div class="sidebar-label">Scenarios</div>')
    for i, s in enumerate(scenarios):
        name = s.get("name", f"Scenario {i + 1}")
        items.append(f'<a class="nav-link" href="#scenario-{i}">{name}</a>')
    return "\n".join(items)


def strip_heavy_data(data):
    """Return a copy with performance time series and violations removed."""
    stripped = {}
    for key, value in data.items():
        if key == "scenarioResults":
            stripped_scenarios = []
            for scenario in value:
                stripped_scenario = {"name": scenario.get("name", "")}
                stripped_tests = []
                for test in scenario.get("tests", []):
                    stripped_test = {}
                    for tk, tv in test.items():
                        if tk == "performance":
                            continue
                        if tk == "correctness":
                            stripped_corr = {k: v for k, v in tv.items()
                                             if k not in ("violations", "lostMessageRanges")}
                            stripped_test[tk] = stripped_corr
                        else:
                            stripped_test[tk] = tv
                    stripped_tests.append(stripped_test)
                stripped_scenario["tests"] = stripped_tests
                stripped_scenarios.append(stripped_scenario)
            stripped["scenarioConfigurations"] = stripped_scenarios
        else:
            stripped[key] = value
    return stripped


def build_config_section(data):
    stripped = strip_heavy_data(data)
    config_json = json.dumps(stripped, indent=2)
    return f"""
    <section id="config-section" class="scenario-section">
        <h2>Configuration</h2>
        <div class="config-json-wrap">
            <pre class="config-json">{config_json}</pre>
        </div>
    </section>
    """


def build_correctness_summary(data):
    """Build a summary table showing correctness results per test."""
    scenarios = data.get("scenarioResults", [])
    rows = []
    for scenario in scenarios:
        scenario_name = scenario.get("name", "")
        for test in scenario.get("tests", []):
            corr = test.get("correctness", {})
            passed = corr.get("passed", False)
            status_class = "status-pass" if passed else "status-fail"
            status_text = "PASSED" if passed else "FAILED"
            rows.append(
                f'<tr>'
                f'<td>{scenario_name}</td>'
                f'<td>{test.get("label", "")}</td>'
                f'<td class="{status_class}">{status_text}</td>'
                f'<td class="num">{corr.get("totalSent", 0):,}</td>'
                f'<td class="num">{corr.get("totalReceived", 0):,}</td>'
                f'<td class="num">{corr.get("lostMessages", 0):,}</td>'
                f'<td class="num">{corr.get("duplicateCount", 0):,}</td>'
                f'<td class="num">{corr.get("outOfOrderCount", 0):,}</td>'
                f'<td class="num">{corr.get("crcFailureCount", 0):,}</td>'
                f'</tr>'
            )

    if not rows:
        return ""

    return f"""
    <section id="correctness-summary" class="scenario-section">
        <h2>Correctness Summary</h2>
        <div class="config-table-wrap">
            <table class="config-table">
                <thead><tr>
                    <th>Scenario</th><th>Test</th><th>Status</th>
                    <th>Sent</th><th>Received</th><th>Lost</th>
                    <th>Duplicates</th><th>Out-of-Order</th><th>CRC Failures</th>
                </tr></thead>
                <tbody>{"".join(rows)}</tbody>
            </table>
        </div>
    </section>
    """


def build_violations_html(tests):
    """Build a violations table for tests that have violations."""
    all_rows = []
    for test in tests:
        label = test.get("label", "")
        corr = test.get("correctness", {})
        violations = corr.get("violations", [])
        lost_ranges = corr.get("lostMessageRanges", [])

        for v in violations:
            v_type = v.get("type", "")
            from_seq = v.get("fromSequence", v.get("sequence", ""))
            to_seq = v.get("toSequence", from_seq)
            count = v.get("count", 1)
            seq_display = str(from_seq) if from_seq == to_seq else f"{from_seq}-{to_seq} ({count})"
            all_rows.append(
                f'<tr>'
                f'<td>{label}</td>'
                f'<td class="vtype-{v_type.lower()}">{v_type}</td>'
                f'<td class="num">{v.get("producerId", "")}</td>'
                f'<td>{v.get("topic", "")}</td>'
                f'<td class="num">{seq_display}</td>'
                f'<td class="num">{v.get("partition", "")}</td>'
                f'<td class="num">{v.get("highSequence", "")}</td>'
                f'</tr>'
            )

        for lr in lost_ranges:
            from_seq = lr.get("fromSequence", 0)
            to_seq = lr.get("toSequence", from_seq)
            count = lr.get("count", 1)
            seq_display = str(from_seq) if from_seq == to_seq else f"{from_seq}-{to_seq} ({count})"
            all_rows.append(
                f'<tr>'
                f'<td>{label}</td>'
                f'<td class="vtype-loss">LOSS</td>'
                f'<td class="num">{lr.get("producerId", "")}</td>'
                f'<td>{lr.get("topic", "")}</td>'
                f'<td class="num">{seq_display}</td>'
                f'<td></td>'
                f'<td></td>'
                f'</tr>'
            )

    if not all_rows:
        return '<p class="subsection-note">No violations detected</p>'

    # Cap display at 200 rows
    truncated = ""
    if len(all_rows) > 200:
        truncated = f'<p class="subsection-note">{len(all_rows)} total violations — showing first 200</p>'
        all_rows = all_rows[:200]

    return f"""
    <div class="config-table-wrap">
        <table class="config-table">
            <thead><tr>
                <th>Test</th><th>Type</th><th>Producer</th><th>Topic</th>
                <th>Sequence</th><th>Partition</th><th>High Sequence</th>
            </tr></thead>
            <tbody>{"".join(all_rows)}</tbody>
        </table>
    </div>
    {truncated}
    """


def build_scenario_sections(scenarios, chart_counts):
    sections = []
    for s_idx, scenario in enumerate(scenarios):
        name = scenario.get("name", f"Scenario {s_idx + 1}")
        tests = scenario.get("tests", [])
        num_charts = chart_counts.get(s_idx, 0)

        # Workload config table
        table_rows = []
        for t in tests:
            wc = t.get("workloadConfig", {})
            table_rows.append(
                f'<tr>'
                f'<td>{t.get("label", "")}</td>'
                f'<td>{wc.get("topics", "")}</td>'
                f'<td>{wc.get("partitionsPerTopic", "")}</td>'
                f'<td>{wc.get("messageSize", "")}</td>'
                f'<td>{wc.get("producersPerTopic", "")}</td>'
                f'<td>{wc.get("producerRate", "")}</td>'
                f'<td>{wc.get("consumersPerGroup", "")}</td>'
                f'</tr>'
            )

        table_html = ""
        if table_rows:
            table_html = f"""
            <div class="config-table-wrap">
                <table class="config-table">
                    <thead><tr>
                        <th>Label</th><th>Topics</th><th>Partitions</th>
                        <th>Msg Size</th><th>Producers</th><th>Rate</th>
                        <th>Consumers/Group</th>
                    </tr></thead>
                    <tbody>{"".join(table_rows)}</tbody>
                </table>
            </div>
            """

        # Performance summary table
        perf_rows = []
        for t in tests:
            r = t.get("performance", {})
            pub_rate = avg(r.get("publishRate", []))
            con_rate = avg(r.get("consumeRate", []))

            def fmt_lat(val):
                if val is None or val == 0:
                    return "-"
                return f"{val:.2f}"

            def fmt_rate(val):
                return f"{val:,.0f}"

            perf_rows.append(
                f'<tr>'
                f'<td>{t.get("label", "")}</td>'
                f'<td class="num">{fmt_rate(pub_rate)}</td>'
                f'<td class="num">{fmt_rate(con_rate)}</td>'
                f'<td class="num">{fmt_lat(r.get("aggregatedPublishLatency50pct"))}</td>'
                f'<td class="num">{fmt_lat(r.get("aggregatedPublishLatency99pct"))}</td>'
                f'<td class="num">{fmt_lat(r.get("aggregatedPublishLatency999pct"))}</td>'
                f'<td class="num">{fmt_lat(r.get("aggregatedPublishLatencyMax"))}</td>'
                f'<td class="num">{fmt_lat(r.get("aggregatedEndToEndLatency50pct"))}</td>'
                f'<td class="num">{fmt_lat(r.get("aggregatedEndToEndLatency99pct"))}</td>'
                f'<td class="num">{fmt_lat(r.get("aggregatedEndToEndLatency999pct"))}</td>'
                f'<td class="num">{fmt_lat(r.get("aggregatedEndToEndLatencyMax"))}</td>'
                f'</tr>'
            )

        perf_table_html = ""
        if perf_rows:
            perf_table_html = f"""
            <div class="config-table-wrap">
                <table class="config-table perf-table">
                    <thead>
                        <tr>
                            <th rowspan="2">Label</th>
                            <th colspan="2">Rate (msg/s)</th>
                            <th colspan="4">Publish Latency (ms)</th>
                            <th colspan="4">E2E Latency (ms)</th>
                        </tr>
                        <tr>
                            <th>pub</th><th>cons</th>
                            <th>p50</th><th>p99</th><th>p99.9</th><th>max</th>
                            <th>p50</th><th>p99</th><th>p99.9</th><th>max</th>
                        </tr>
                    </thead>
                    <tbody>{"".join(perf_rows)}</tbody>
                </table>
            </div>
            """

        # Charts
        chart_cards = []
        for c_idx in range(num_charts):
            chart_id = f"chart-{s_idx}-{c_idx}"
            chart_cards.append(
                f'<div class="chart-card"><canvas id="{chart_id}"></canvas></div>'
            )

        # Violations
        violations_html = build_violations_html(tests)

        sections.append(f"""
        <section id="scenario-{s_idx}" class="scenario-section">
            <h2>Scenario: {name}</h2>
            {table_html}
            {perf_table_html}
            <h3 class="subsection-heading">Performance</h3>
            <div class="chart-grid">
                {"".join(chart_cards)}
            </div>
            <h3 class="subsection-heading">Violations</h3>
            {violations_html}
        </section>
        """)

    return "\n".join(sections)


def build_html(data, chart_configs, chart_counts):
    metadata = data.get("metadata", {})
    config = data.get("config", {})
    correctness_config = config.get("correctnessConfig", {})
    scenarios = data.get("scenarioResults", [])

    workload_name = metadata.get("name", "Unknown Workload")
    kafka_config_name = metadata.get("kafkaConfigName", "")
    environment = metadata.get("environment", "")
    system = metadata.get("system", "")
    begin_time = metadata.get("beginTime", "")
    end_time = metadata.get("endTime", "")
    test_mode = correctness_config.get("testMode", "CONCURRENT")
    test_duration = correctness_config.get("testDurationMinutes", "")
    grace_period = correctness_config.get("gracePeriodSeconds", "")
    annotation_meta = metadata_annotation_html(metadata)

    sidebar_html = build_sidebar_html(scenarios)
    correctness_summary_html = build_correctness_summary(data)
    config_section_html = build_config_section(data)
    sections_html = build_scenario_sections(scenarios, chart_counts)
    configs_json = json.dumps(chart_configs)
    ptick_json = json.dumps({
        "values": PERCENTILE_TICK_VALUES,
        "labels": PERCENTILE_TICK_LABELS,
    })

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Dimster Correctness - {workload_name}</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<script src="https://cdn.jsdelivr.net/npm/hammerjs@2.0.8"></script>
<script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-zoom"></script>
<style>
*, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
body {{
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
    background: #f8f9fa;
    color: #1a1a2e;
    display: flex;
    min-height: 100vh;
}}
.sidebar {{
    position: fixed;
    top: 0;
    left: 0;
    width: 220px;
    height: 100vh;
    background: #1a1a2e;
    color: #c8c8d4;
    padding: 24px 0;
    overflow-y: auto;
    z-index: 100;
}}
.sidebar-title {{
    font-size: 15px;
    font-weight: 700;
    color: #fff;
    padding: 0 20px 16px;
    border-bottom: 1px solid #2a2a4e;
    margin-bottom: 12px;
    letter-spacing: 0.5px;
}}
.sidebar-label {{
    font-size: 10px;
    text-transform: uppercase;
    letter-spacing: 1px;
    color: #6c6c8a;
    padding: 12px 20px 6px;
}}
.nav-link {{
    display: block;
    padding: 8px 20px;
    color: #a0a0b8;
    text-decoration: none;
    font-size: 13px;
    transition: background 0.15s, color 0.15s;
    border-left: 3px solid transparent;
}}
.nav-link:hover {{
    background: #2a2a4e;
    color: #fff;
    border-left-color: #4e79a7;
}}
.main {{
    margin-left: 220px;
    flex: 1;
    padding: 32px 40px;
    max-width: 1400px;
}}
.header {{
    background: #fff;
    border-radius: 10px;
    box-shadow: 0 1px 4px rgba(0,0,0,0.06);
    padding: 28px 32px;
    margin-bottom: 32px;
}}
.header h1 {{
    font-size: 22px;
    font-weight: 700;
    margin-bottom: 16px;
    color: #1a1a2e;
}}
.meta-grid {{
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
    gap: 10px 24px;
}}
.meta-item {{
    font-size: 13px;
}}
.meta-item .label {{
    color: #6c6c8a;
    font-weight: 500;
}}
.meta-item .value {{
    color: #1a1a2e;
    font-weight: 600;
}}
.scenario-section {{
    margin-bottom: 48px;
}}
.scenario-section h2 {{
    font-size: 18px;
    font-weight: 700;
    margin-bottom: 16px;
    padding-bottom: 8px;
    border-bottom: 2px solid #e0e0e0;
}}
.subsection-heading {{
    font-size: 15px;
    font-weight: 600;
    color: #4a4a6a;
    margin-top: 28px;
    margin-bottom: 12px;
    padding-bottom: 6px;
    border-bottom: 1px solid #e8e8e8;
}}
.subsection-note {{
    font-size: 13px;
    color: #8a8a9a;
    font-style: italic;
    padding: 12px 16px;
    background: #f0f1f3;
    border-radius: 6px;
    margin-bottom: 16px;
}}
.config-table-wrap {{
    overflow-x: auto;
    margin-bottom: 20px;
}}
.config-table {{
    width: 100%;
    border-collapse: collapse;
    font-size: 12px;
    background: #fff;
    border-radius: 8px;
    overflow: hidden;
    box-shadow: 0 1px 3px rgba(0,0,0,0.05);
}}
.config-table th {{
    background: #f0f1f3;
    padding: 8px 12px;
    text-align: left;
    font-weight: 600;
    color: #4a4a6a;
    white-space: nowrap;
}}
.config-table td {{
    padding: 7px 12px;
    border-top: 1px solid #eee;
    white-space: nowrap;
}}
.config-table tr:hover td {{
    background: #f8f9ff;
}}
.config-table td.num {{
    text-align: right;
    font-variant-numeric: tabular-nums;
}}
.perf-table thead tr:first-child th[colspan] {{
    text-align: center;
    border-bottom: 1px solid #d0d0d8;
}}
.perf-table thead tr:nth-child(2) th {{
    font-weight: 500;
    font-size: 11px;
    color: #6a6a8a;
}}
.status-pass {{
    color: #2d8a4e;
    font-weight: 700;
}}
.status-fail {{
    color: #c0392b;
    font-weight: 700;
}}
.vtype-duplicate {{
    color: #e67e22;
    font-weight: 600;
}}
.vtype-out_of_order, .vtype-out-of-order {{
    color: #8e44ad;
    font-weight: 600;
}}
.vtype-loss {{
    color: #c0392b;
    font-weight: 600;
}}
.vtype-crc {{
    color: #e74c3c;
    font-weight: 600;
}}
.chart-grid {{
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 20px;
}}
.chart-card {{
    background: #fff;
    border-radius: 8px;
    box-shadow: 0 1px 4px rgba(0,0,0,0.06);
    padding: 20px;
}}
.chart-card canvas {{
    width: 100% !important;
}}
.config-json-wrap {{
    overflow-x: auto;
    margin-bottom: 20px;
    max-height: 500px;
    overflow-y: auto;
}}
.config-json {{
    background: #fff;
    border-radius: 8px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.05);
    padding: 20px 24px;
    font-family: 'SF Mono', 'Fira Code', 'Consolas', monospace;
    font-size: 12px;
    line-height: 1.6;
    color: #1a1a2e;
    white-space: pre-wrap;
    word-break: break-word;
}}
.reset-bar {{
    display: none;
    position: sticky;
    top: 8px;
    z-index: 50;
    background: #fff;
    border: 1px solid #e0e0e0;
    border-radius: 8px;
    box-shadow: 0 1px 4px rgba(0,0,0,0.06);
    padding: 8px 16px;
    margin-bottom: 12px;
    align-items: center;
    gap: 12px;
    font-size: 13px;
    color: #4a4a6a;
}}
.reset-bar.visible {{
    display: flex;
}}
.reset-bar button {{
    background: #4e79a7;
    color: #fff;
    border: none;
    border-radius: 5px;
    padding: 5px 14px;
    font-size: 12px;
    font-weight: 600;
    cursor: pointer;
    transition: background 0.15s;
}}
.reset-bar button:hover {{
    background: #3b6291;
}}
@media (max-width: 900px) {{
    .chart-grid {{ grid-template-columns: 1fr; }}
    .sidebar {{ display: none; }}
    .main {{ margin-left: 0; }}
}}
</style>
</head>
<body>

<nav class="sidebar">
    <div class="sidebar-title">Correctness</div>
    {sidebar_html}
</nav>

<div class="main">
    <div class="header">
        <h1>{workload_name}</h1>
        <div class="meta-grid">
            <div class="meta-item"><span class="label">System:</span> <span class="value">{system}</span></div>
            <div class="meta-item"><span class="label">Environment:</span> <span class="value">{environment}</span></div>
            <div class="meta-item"><span class="label">Kafka Config:</span> <span class="value">{kafka_config_name}</span></div>
            <div class="meta-item"><span class="label">Test Mode:</span> <span class="value">{test_mode}</span></div>
            <div class="meta-item"><span class="label">Test Duration:</span> <span class="value">{test_duration} min</span></div>
            <div class="meta-item"><span class="label">Grace Period:</span> <span class="value">{grace_period}s</span></div>
            {annotation_meta}
            <div class="meta-item"><span class="label">Start:</span> <span class="value">{begin_time}</span></div>
            <div class="meta-item"><span class="label">End:</span> <span class="value">{end_time}</span></div>
        </div>
    </div>

    <div id="resetBar-time" class="reset-bar">
        <span>Zoomed — drag on any time chart to adjust</span>
        <button onclick="resetZoom('time')">Reset Zoom</button>
    </div>
    <div id="resetBar-percentile" class="reset-bar">
        <span>Zoomed — drag on any percentile chart to adjust</span>
        <button onclick="resetZoom('percentile')">Reset Zoom</button>
    </div>

    {correctness_summary_html}
    {config_section_html}
    {sections_html}
</div>

<script>
const PTICK_MAP = {ptick_json};

function xToPercentile(x) {{
    return 100.0 - 100.0 / Math.pow(10, x);
}}

function applyPercentileCallbacks(opts) {{
    if (opts.scales && opts.scales.x && opts.scales.x.ticks && opts.scales.x.ticks.__percentile_ticks) {{
        const tickVals = PTICK_MAP.values;
        const tickLabels = PTICK_MAP.labels;
        opts.scales.x.afterBuildTicks = function(axis) {{
            axis.ticks = tickVals
                .filter(v => v >= axis.min && v <= axis.max)
                .map(v => ({{ value: v }}));
        }};
        opts.scales.x.ticks = {{
            callback: function(value) {{
                const idx = tickVals.indexOf(value);
                return idx >= 0 ? tickLabels[idx] : '';
            }}
        }};
    }}
    if (opts.plugins && opts.plugins.tooltip && opts.plugins.tooltip.callbacks
        && opts.plugins.tooltip.callbacks.__percentile_tooltip) {{
        opts.plugins.tooltip.callbacks = {{
            title: function(items) {{
                if (!items.length) return '';
                const p = xToPercentile(items[0].parsed.x);
                if (p >= 99.99) return 'p99.99';
                if (p >= 99.9) return 'p' + p.toFixed(1);
                if (p >= 99) return 'p' + p.toFixed(0);
                return 'p' + p.toFixed(0);
            }},
            label: function(ctx) {{
                return ctx.dataset.label + ': ' + ctx.parsed.y.toFixed(2) + ' ms';
            }}
        }};
    }}
}}

var zoomGroups = {{ time: {{}}, percentile: {{}} }};

function syncZoom(sourceChart, group) {{
    var xScale = sourceChart.scales.x;
    var min = xScale.min, max = xScale.max;
    Object.values(zoomGroups[group]).forEach(function(c) {{
        if (c !== sourceChart) c.zoomScale('x', {{min: min, max: max}}, 'none');
    }});
    document.getElementById('resetBar-' + group).classList.add('visible');
}}

function resetZoom(group) {{
    Object.values(zoomGroups[group]).forEach(function(c) {{ c.resetZoom('none'); }});
    document.getElementById('resetBar-' + group).classList.remove('visible');
}}

function applyZoomCallbacks(config, group) {{
    if (!config.options.plugins) config.options.plugins = {{}};
    config.options.plugins.zoom = {{
        zoom: {{
            drag: {{
                enabled: true,
                backgroundColor: 'rgba(78,121,167,0.15)',
                borderColor: '#4e79a7',
                borderWidth: 1
            }},
            mode: 'x',
            onZoomComplete: function(ctx) {{ syncZoom(ctx.chart, group); }}
        }}
    }};
}}

const chartConfigs = {configs_json};
chartConfigs.forEach(c => {{
    applyPercentileCallbacks(c.config.options);
    if (c.zoomGroup) applyZoomCallbacks(c.config, c.zoomGroup);
    const ctx = document.getElementById(c.id);
    if (ctx) {{
        var chart = new Chart(ctx, c.config);
        if (c.zoomGroup) zoomGroups[c.zoomGroup][c.id] = chart;
    }}
}});
</script>

</body>
</html>"""


def main():
    parser = argparse.ArgumentParser(
        description="Generate HTML charts from Dimster correctness result JSON files."
    )
    parser.add_argument(
        "--results", required=True, help="Directory to scan for result JSON files"
    )
    parser.add_argument(
        "--output", required=True, help="Directory to write HTML files"
    )
    args = parser.parse_args()

    if not os.path.isdir(args.results):
        print(f"Error: results directory does not exist: {args.results}", file=sys.stderr)
        sys.exit(1)

    os.makedirs(args.output, exist_ok=True)

    files = find_correctness_json_files(args.results)
    if not files:
        sys.exit(0)

    for path, data in files:
        rel = os.path.relpath(path, args.results)
        out_name = os.path.splitext(rel)[0] + ".html"
        out_path = os.path.join(args.output, out_name)

        os.makedirs(os.path.dirname(out_path), exist_ok=True)

        chart_configs, chart_counts = build_chart_configs(data)
        html = build_html(data, chart_configs, chart_counts)

        with open(out_path, "w") as fh:
            fh.write(html)

        print(f"Generated: {out_path}")

    print(f"Done. Processed {len(files)} correctness file(s).")


if __name__ == "__main__":
    main()
