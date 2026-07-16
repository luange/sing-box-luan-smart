#!/usr/bin/env python3

import argparse
import json
import os
import signal
import tempfile
import time


VALID_A = """proxies:
  - name: lab-a
    type: socks5
    server: 10.254.40.118
    port: 20001
    udp: true
  - name: lab-b
    type: socks5
    server: 10.254.40.118
    port: 20002
    udp: true
"""

VALID_B = """proxies:
  - name: lab-b
    type: socks5
    server: 10.254.40.118
    port: 20001
    udp: true
  - name: lab-a
    type: socks5
    server: 10.254.40.118
    port: 20002
    udp: true
"""

INVALID = "this is not a subscription\n"


def atomic_write(path, content):
    directory = os.path.dirname(path) or "."
    with tempfile.NamedTemporaryFile("w", dir=directory, delete=False) as output:
        output.write(content)
        temporary_path = output.name
    os.replace(temporary_path, path)


def wait_for_log(log_file, accepted, timeout):
    deadline = time.monotonic() + timeout
    lines = []
    while time.monotonic() < deadline:
        line = log_file.readline()
        if not line:
            time.sleep(0.005)
            continue
        lines.append(line.strip())
        if any(marker in line for marker in accepted):
            return line.strip(), lines
    raise TimeoutError("log completion timed out: " + repr(lines[-10:]))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pid-file", required=True)
    parser.add_argument("--log-file", required=True)
    parser.add_argument("--provider-file", required=True)
    parser.add_argument("--result", required=True)
    parser.add_argument("--cycles", type=int, default=100)
    parser.add_argument("--timeout", type=float, default=10.0)
    args = parser.parse_args()

    with open(args.pid_file, encoding="ascii") as pid_file:
        pid = int(pid_file.read().strip())
    os.kill(pid, 0)

    result = {
        "cycles": args.cycles,
        "valid_reload_completed": 0,
        "invalid_reload_rejected": 0,
        "errors": [],
        "started_at": time.time(),
    }
    last_valid = VALID_A
    with open(args.log_file, encoding="utf-8", errors="replace") as log_file:
        log_file.seek(0, os.SEEK_END)
        for index in range(args.cycles):
            valid_round = index % 2 == 0
            content = VALID_A if index % 4 == 0 else VALID_B
            if valid_round:
                last_valid = content
                atomic_write(args.provider_file, content)
                time.sleep(0.01)
                os.kill(pid, signal.SIGHUP)
                try:
                    wait_for_log(log_file, ["sing-box configuration reloaded"], args.timeout)
                    result["valid_reload_completed"] += 1
                except Exception as error:
                    result["errors"].append({"iteration": index + 1, "phase": "valid", "error": repr(error)})
                    break
            else:
                atomic_write(args.provider_file, INVALID)
                time.sleep(0.01)
                os.kill(pid, signal.SIGHUP)
                try:
                    wait_for_log(log_file, ["reload service: prepare runtime generation"], args.timeout)
                    result["invalid_reload_rejected"] += 1
                except Exception as error:
                    result["errors"].append({"iteration": index + 1, "phase": "invalid", "error": repr(error)})
                    break
                finally:
                    atomic_write(args.provider_file, last_valid)
                    time.sleep(0.02)

    atomic_write(args.provider_file, last_valid)
    result["finished_at"] = time.time()
    result["elapsed_seconds"] = round(result["finished_at"] - result["started_at"], 3)
    result["success"] = (
        result["valid_reload_completed"] == (args.cycles + 1) // 2
        and result["invalid_reload_rejected"] == args.cycles // 2
        and not result["errors"]
    )
    atomic_write(args.result, json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
