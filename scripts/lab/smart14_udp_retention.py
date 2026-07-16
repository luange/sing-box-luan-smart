#!/usr/bin/env python3

import argparse
import json
import time

from smart14_reload_load import socks_udp_associate, socks_udp_exchange


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--proxy-host", default="10.254.40.117")
    parser.add_argument("--proxy-port", required=True, type=int)
    parser.add_argument("--target-host", default="10.254.40.118")
    parser.add_argument("--target-port", default=19082, type=int)
    parser.add_argument("--count", default=500, type=int)
    args = parser.parse_args()

    started = time.time()
    errors = []
    for sequence in range(args.count):
        control = None
        udp = None
        try:
            control, udp, relay = socks_udp_associate((args.proxy_host, args.proxy_port))
            payload = ("smart14-retention-%08d" % sequence).encode()
            if socks_udp_exchange(udp, relay, (args.target_host, args.target_port), payload) != payload:
                raise AssertionError("UDP payload mismatch")
        except Exception as error:
            errors.append(repr(error))
            if len(errors) >= 10:
                break
        finally:
            if udp is not None:
                udp.close()
            if control is not None:
                control.close()
    result = {
        "requested": args.count,
        "completed": sequence + 1 - len(errors),
        "elapsed_seconds": round(time.time() - started, 3),
        "errors": errors,
    }
    result["success"] = not errors and result["completed"] == args.count
    print(json.dumps(result, sort_keys=True))
    if not result["success"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
