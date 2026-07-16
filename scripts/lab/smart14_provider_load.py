#!/usr/bin/env python3

import argparse
import json
import threading
import time

import smart14_reload_load as common


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--api", default="http://10.254.40.117:19141")
    parser.add_argument("--proxy-host", default="10.254.40.117")
    parser.add_argument("--proxy-port", type=int, default=18141)
    parser.add_argument("--target-host", default="10.254.40.118")
    parser.add_argument("--duration", type=float, default=60.0)
    parser.add_argument("--result", required=True)
    args = parser.parse_args()

    proxy = (args.proxy_host, args.proxy_port)
    results = common.Results()
    stop = threading.Event()
    workers = [
        (common.proxy_state_worker, (stop, results, args.api, "SMART-PROVIDER", "lab-provider/lab-a", "♻️ 智能选择")),
        (common.delay_worker, (stop, results, args.api, "♻️ 智能选择", "https://10.254.40.118:19443/generate_204")),
        (common.http_worker, (stop, results, proxy, (args.target_host, 19080), False)),
        (common.http_worker, (stop, results, proxy, (args.target_host, 19443), True)),
        (common.persistent_tcp_worker, (stop, results, proxy, (args.target_host, 19081))),
        (common.new_tcp_worker, (stop, results, proxy, (args.target_host, 19081))),
        (common.persistent_udp_worker, (stop, results, proxy, (args.target_host, 19082))),
        (common.new_udp_worker, (stop, results, proxy, (args.target_host, 19082))),
        (common.dns_worker, (stop, results, proxy, (args.target_host, 19053))),
    ]
    threads = [threading.Thread(target=target, args=arguments, daemon=True) for target, arguments in workers]
    started = time.time()
    for thread in threads:
        thread.start()
    time.sleep(args.duration)
    stop.set()
    for thread in threads:
        thread.join(timeout=10.0)

    minimums = {
        "api_state_SMART-PROVIDER": 10,
        "delay_♻️ 智能选择": 10,
        "http": 10,
        "https": 10,
        "tcp_persistent": 10,
        "tcp_new": 10,
        "udp_persistent": 10,
        "udp_new": 10,
        "dns": 10,
    }
    summary = {
        "started_at": started,
        "finished_at": time.time(),
        "elapsed_seconds": round(time.time() - started, 3),
        "counts": results.counts,
        "errors": results.errors,
        "threads_alive": sum(thread.is_alive() for thread in threads),
        "minimums": minimums,
    }
    summary["error_counts"] = {
        key: value for key, value in results.counts.items() if key.endswith("_errors") and value
    }
    summary["below_minimum"] = {
        key: minimum for key, minimum in minimums.items() if results.counts.get(key, 0) < minimum
    }
    summary["success"] = not summary["error_counts"] and not summary["below_minimum"] and summary["threads_alive"] == 0
    with open(args.result, "w", encoding="utf-8") as output:
        json.dump(summary, output, indent=2, sort_keys=True)
        output.write("\n")
    print(json.dumps(summary, sort_keys=True))


if __name__ == "__main__":
    main()
