#!/usr/bin/env python3

import argparse
import glob
import json
import statistics


OPERATIONS = ("api", "http", "https", "tcp", "udp")
METRICS = ("ops_per_second", "p50_ms", "p95_ms", "p99_ms")


def aggregate(results):
    summary = {}
    for name in ("baseline", "candidate"):
        selected = [result for result in results if result["name"] == name]
        summary[name] = {
            operation: {
                metric: round(statistics.median(result["operations"][operation][metric] for result in selected), 3)
                for metric in METRICS
            }
            for operation in OPERATIONS
        }
    summary["ratio_candidate_over_baseline"] = {
        operation: {
            metric: round(summary["candidate"][operation][metric] / summary["baseline"][operation][metric], 4)
            for metric in METRICS
        }
        for operation in OPERATIONS
    }
    return summary


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pattern", default="/root/smart14-alpha44-test/ab-*.json")
    parser.add_argument("--expected", type=int, required=True)
    parser.add_argument("--output")
    args = parser.parse_args()

    paths = sorted(glob.glob(args.pattern))
    if len(paths) != args.expected:
        raise SystemExit("expected %d result files, found %d" % (args.expected, len(paths)))
    results = [json.load(open(path, encoding="utf-8")) for path in paths]
    invalid = [path for path, result in zip(paths, results) if not result["success"] or result["errors"] or result["threads_alive"]]
    if invalid:
        raise SystemExit("invalid A/B results: " + ", ".join(invalid))
    counts = {name: sum(result["name"] == name for result in results) for name in ("baseline", "candidate")}
    if counts["baseline"] != counts["candidate"]:
        raise SystemExit("unbalanced A/B results: " + repr(counts))

    summary = aggregate(results)
    summary["all_success"] = True
    summary["runs"] = counts
    content = json.dumps(summary, indent=2, sort_keys=True) + "\n"
    if args.output:
        with open(args.output, "w", encoding="utf-8") as output:
            output.write(content)
    print(content, end="")


if __name__ == "__main__":
    main()
