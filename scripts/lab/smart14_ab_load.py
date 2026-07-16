#!/usr/bin/env python3

import argparse
import json
import math
import threading
import time
import urllib.request

from smart14_reload_load import (
    connect_tunnel,
    http_proxy_request,
    recv_exact,
    socks_udp_associate,
    socks_udp_exchange,
)


class Measurements:
    def __init__(self):
        self.lock = threading.Lock()
        self.latencies = {}
        self.bytes = {}
        self.errors = {}

    def success(self, name, started, byte_count=0):
        elapsed_ms = (time.perf_counter() - started) * 1000
        with self.lock:
            self.latencies.setdefault(name, []).append(elapsed_ms)
            self.bytes[name] = self.bytes.get(name, 0) + byte_count

    def failure(self, name):
        with self.lock:
            self.errors[name] = self.errors.get(name, 0) + 1


def percentile(values, quantile):
    if not values:
        return None
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, math.ceil(len(ordered) * quantile) - 1))
    return round(ordered[index], 3)


def api_worker(stop, measurements, api):
    while not stop.is_set():
        started = time.perf_counter()
        try:
            with urllib.request.urlopen(api + "/proxies", timeout=5) as response:
                if response.status != 200 or not json.load(response).get("proxies"):
                    raise AssertionError("invalid proxies response")
            measurements.success("api", started)
        except Exception:
            measurements.failure("api")


def http_worker(stop, measurements, proxy, target, tls):
    name = "https" if tls else "http"
    while not stop.is_set():
        started = time.perf_counter()
        try:
            http_proxy_request(proxy, target, tls=tls)
            measurements.success(name, started, 65536)
        except Exception:
            measurements.failure(name)


def tcp_worker(stop, measurements, proxy, target):
    sequence = 0
    while not stop.is_set():
        sock = None
        started = time.perf_counter()
        try:
            sock = connect_tunnel(proxy, target)
            payload = ("smart14-ab-tcp-%08d" % sequence).encode()
            sock.sendall(payload)
            if recv_exact(sock, len(payload)) != payload:
                raise AssertionError("TCP payload mismatch")
            measurements.success("tcp", started, len(payload) * 2)
            sequence += 1
        except Exception:
            measurements.failure("tcp")
        finally:
            if sock is not None:
                sock.close()


def udp_worker(stop, measurements, proxy, target):
    sequence = 0
    while not stop.is_set():
        control = None
        udp = None
        started = time.perf_counter()
        try:
            control, udp, relay = socks_udp_associate(proxy)
            payload = ("smart14-ab-udp-%08d" % sequence).encode()
            if socks_udp_exchange(udp, relay, target, payload) != payload:
                raise AssertionError("UDP payload mismatch")
            measurements.success("udp", started, len(payload) * 2)
            sequence += 1
        except Exception:
            measurements.failure("udp")
        finally:
            if udp is not None:
                udp.close()
            if control is not None:
                control.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--name", required=True)
    parser.add_argument("--api", required=True)
    parser.add_argument("--proxy-host", default="10.254.40.117")
    parser.add_argument("--proxy-port", required=True, type=int)
    parser.add_argument("--target-host", default="10.254.40.118")
    parser.add_argument("--duration", default=15.0, type=float)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    proxy = (args.proxy_host, args.proxy_port)
    measurements = Measurements()
    stop = threading.Event()
    workers = [
        (api_worker, (stop, measurements, args.api)),
        (http_worker, (stop, measurements, proxy, (args.target_host, 19080), False)),
        (http_worker, (stop, measurements, proxy, (args.target_host, 19443), True)),
        (tcp_worker, (stop, measurements, proxy, (args.target_host, 19081))),
        (udp_worker, (stop, measurements, proxy, (args.target_host, 19082))),
    ]
    threads = [threading.Thread(target=target, args=arguments) for target, arguments in workers]
    started = time.time()
    for thread in threads:
        thread.start()
    time.sleep(args.duration)
    stop.set()
    for thread in threads:
        thread.join(timeout=10)
    elapsed = time.time() - started

    operations = {}
    for name, values in measurements.latencies.items():
        operations[name] = {
            "count": len(values),
            "ops_per_second": round(len(values) / elapsed, 3),
            "p50_ms": percentile(values, 0.50),
            "p95_ms": percentile(values, 0.95),
            "p99_ms": percentile(values, 0.99),
            "bytes_per_second": round(measurements.bytes.get(name, 0) / elapsed, 3),
        }
    result = {
        "name": args.name,
        "elapsed_seconds": round(elapsed, 3),
        "operations": operations,
        "errors": measurements.errors,
        "threads_alive": sum(thread.is_alive() for thread in threads),
    }
    result["success"] = (
        not measurements.errors
        and not result["threads_alive"]
        and all(operations.get(name, {}).get("count", 0) >= 10 for name in ("api", "http", "https", "tcp", "udp"))
    )
    with open(args.output, "w", encoding="utf-8") as output:
        json.dump(result, output, indent=2, sort_keys=True)
        output.write("\n")
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
