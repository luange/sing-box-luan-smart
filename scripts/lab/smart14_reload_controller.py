#!/usr/bin/env python3

import argparse
import json
import os
import signal
import tempfile
import time


def write_result(path, result):
    directory = os.path.dirname(path) or "."
    os.makedirs(directory, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", dir=directory, delete=False) as output:
        json.dump(result, output, indent=2, sort_keys=True)
        output.write("\n")
        temporary_path = output.name
    os.replace(temporary_path, path)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pid-file", required=True)
    parser.add_argument("--log-file", required=True)
    parser.add_argument("--result", required=True)
    parser.add_argument("--count", type=int, default=300)
    parser.add_argument("--timeout", type=float, default=10.0)
    parser.add_argument("--interval", type=float, default=0.05)
    args = parser.parse_args()

    with open(args.pid_file, encoding="ascii") as pid_file:
        pid = int(pid_file.read().strip())
    os.kill(pid, 0)

    result = {
        "requested": args.count,
        "completed": 0,
        "timeouts": 0,
        "errors": [],
        "durations_ms": [],
        "started_at": time.time(),
    }
    with open(args.log_file, encoding="utf-8", errors="replace") as log_file:
        log_file.seek(0, os.SEEK_END)
        for index in range(args.count):
            started = time.monotonic()
            os.kill(pid, signal.SIGHUP)
            deadline = started + args.timeout
            reload_error = None
            while time.monotonic() < deadline:
                line = log_file.readline()
                if not line:
                    time.sleep(0.005)
                    continue
                if "reload service:" in line and ("ERROR" in line or "error" in line.lower()):
                    reload_error = line.strip()
                if "sing-box configuration reloaded" in line:
                    result["completed"] += 1
                    result["durations_ms"].append(round((time.monotonic() - started) * 1000, 3))
                    break
            else:
                result["timeouts"] += 1
                result["errors"].append({
                    "iteration": index + 1,
                    "error": reload_error or "reload completion log timed out",
                })
                break
            if reload_error:
                result["errors"].append({"iteration": index + 1, "error": reload_error})
                break
            time.sleep(args.interval)

    durations = result["durations_ms"]
    result["finished_at"] = time.time()
    result["elapsed_seconds"] = round(result["finished_at"] - result["started_at"], 3)
    if durations:
        ordered = sorted(durations)
        result["duration_min_ms"] = ordered[0]
        result["duration_mean_ms"] = round(sum(ordered) / len(ordered), 3)
        result["duration_p95_ms"] = ordered[min(len(ordered) - 1, int(len(ordered) * 0.95))]
        result["duration_max_ms"] = ordered[-1]
    result["success"] = (
        result["completed"] == result["requested"]
        and result["timeouts"] == 0
        and not result["errors"]
    )
    write_result(args.result, result)
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
