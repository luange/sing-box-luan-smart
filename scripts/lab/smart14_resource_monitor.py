#!/usr/bin/env python3

import argparse
import csv
import json
import os
import time
import urllib.request


def read_status(pid):
    values = {}
    with open(f"/proc/{pid}/status", encoding="ascii") as status_file:
        for line in status_file:
            name, separator, value = line.partition(":")
            if not separator:
                continue
            if name in ("VmRSS", "RssAnon"):
                values[name] = int(value.strip().split()[0])
            elif name == "Threads":
                values[name] = int(value.strip())
    values["FD"] = len(os.listdir(f"/proc/{pid}/fd"))
    return values


def api_ok(url):
    try:
        with urllib.request.urlopen(url, timeout=2.0) as response:
            return response.status == 200
    except Exception:
        return False


def metric_summary(rows, name):
    values = [row[name] for row in rows]
    quartile = max(1, len(values) // 4)
    return {
        "first": values[0],
        "minimum": min(values),
        "maximum": max(values),
        "last": values[-1],
        "first_quartile_mean": round(sum(values[:quartile]) / quartile, 3),
        "last_quartile_mean": round(sum(values[-quartile:]) / quartile, 3),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pid-file", required=True)
    parser.add_argument("--api", required=True)
    parser.add_argument("--duration", type=float, default=1815.0)
    parser.add_argument("--interval", type=float, default=5.0)
    parser.add_argument("--csv", required=True)
    parser.add_argument("--summary", required=True)
    args = parser.parse_args()

    with open(args.pid_file, encoding="ascii") as pid_file:
        pid = int(pid_file.read().strip())
    rows = []
    started = time.time()
    deadline = time.monotonic() + args.duration
    while True:
        now = time.time()
        try:
            status = read_status(pid)
            alive = True
        except (FileNotFoundError, ProcessLookupError):
            status = {"VmRSS": 0, "RssAnon": 0, "Threads": 0, "FD": 0}
            alive = False
        rows.append({
            "timestamp": now,
            "elapsed_seconds": round(now - started, 3),
            "alive": int(alive),
            "api_ok": int(api_ok(args.api)),
            **status,
        })
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        time.sleep(min(args.interval, remaining))

    with open(args.csv, "w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    summary = {
        "pid": pid,
        "samples": len(rows),
        "elapsed_seconds": round(rows[-1]["timestamp"] - rows[0]["timestamp"], 3),
        "process_alive_samples": sum(row["alive"] for row in rows),
        "api_success_samples": sum(row["api_ok"] for row in rows),
        "metrics": {name: metric_summary(rows, name) for name in ("VmRSS", "RssAnon", "Threads", "FD")},
    }
    summary["success"] = (
        summary["process_alive_samples"] == summary["samples"]
        and summary["api_success_samples"] == summary["samples"]
    )
    with open(args.summary, "w", encoding="utf-8") as output:
        json.dump(summary, output, indent=2, sort_keys=True)
        output.write("\n")
    print(json.dumps(summary, sort_keys=True))


if __name__ == "__main__":
    main()
