#!/usr/bin/env python3

import argparse
import json
import socket
import ssl
import struct
import threading
import time
import urllib.parse
import urllib.request


class Results:
    def __init__(self):
        self.lock = threading.Lock()
        self.counts = {}
        self.errors = []

    def success(self, name):
        with self.lock:
            self.counts[name] = self.counts.get(name, 0) + 1

    def failure(self, name, error):
        with self.lock:
            key = name + "_errors"
            self.counts[key] = self.counts.get(key, 0) + 1
            if len(self.errors) < 100:
                self.errors.append({"worker": name, "error": repr(error)})


def recv_exact(sock, size):
    content = bytearray()
    while len(content) < size:
        chunk = sock.recv(size - len(content))
        if not chunk:
            raise ConnectionError("unexpected EOF")
        content.extend(chunk)
    return bytes(content)


def recv_until(sock, marker, limit=65536):
    content = bytearray()
    while marker not in content:
        chunk = sock.recv(4096)
        if not chunk:
            raise ConnectionError("unexpected EOF")
        content.extend(chunk)
        if len(content) > limit:
            raise ValueError("response header is too large")
    return bytes(content)


def recv_all(sock):
    content = bytearray()
    while True:
        chunk = sock.recv(65536)
        if not chunk:
            return bytes(content)
        content.extend(chunk)


def api_request(api, method, path, body=None, timeout=5.0):
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(
        api + path,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        content = response.read()
        if response.status == 204:
            return None
        return json.loads(content)


def proxy_state_worker(stop, results, api, group, candidate, automatic):
    encoded_group = urllib.parse.quote(group, safe="")
    while not stop.is_set():
        try:
            api_request(api, "PUT", "/proxies/" + encoded_group, {"name": candidate})
            state = api_request(api, "GET", "/proxies/" + encoded_group)
            if state.get("now") != candidate:
                raise AssertionError("temporary override state mismatch: " + repr(state.get("now")))
            api_request(api, "PUT", "/proxies/" + encoded_group, {"name": automatic})
            state = api_request(api, "GET", "/proxies/" + encoded_group)
            if state.get("now") != automatic:
                raise AssertionError("Smart return state mismatch: " + repr(state.get("now")))
            results.success("api_state_" + group)
        except Exception as error:
            results.failure("api_state_" + group, error)


def delay_worker(stop, results, api, automatic, target_url):
    encoded = urllib.parse.quote(automatic, safe="")
    query = urllib.parse.urlencode({"url": target_url, "timeout": 3000})
    while not stop.is_set():
        try:
            response = api_request(api, "GET", "/proxies/" + encoded + "/delay?" + query, timeout=5.0)
            if not isinstance(response.get("delay"), int) or response["delay"] < 0:
                raise AssertionError("invalid delay response: " + repr(response))
            results.success("delay_" + automatic)
        except Exception as error:
            results.failure("delay_" + automatic, error)


def http_proxy_request(proxy, target, tls=False):
    target_host, target_port = target
    if tls:
        raw = socket.create_connection(proxy, timeout=5.0)
        raw.settimeout(5.0)
        raw.sendall(
            ("CONNECT %s:%d HTTP/1.1\r\nHost: %s:%d\r\nConnection: keep-alive\r\n\r\n" %
             (target_host, target_port, target_host, target_port)).encode()
        )
        header = recv_until(raw, b"\r\n\r\n")
        if not header.startswith(b"HTTP/1.1 200") and not header.startswith(b"HTTP/1.0 200"):
            raise AssertionError("CONNECT failed: " + repr(header[:200]))
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        sock = context.wrap_socket(raw, server_hostname=target_host)
        request_target = "/download?bytes=65536"
    else:
        sock = socket.create_connection(proxy, timeout=5.0)
        sock.settimeout(5.0)
        request_target = "http://%s:%d/download?bytes=65536" % target
    try:
        sock.sendall(
            ("GET %s HTTP/1.1\r\nHost: %s:%d\r\nConnection: close\r\n\r\n" %
             (request_target, target_host, target_port)).encode()
        )
        response = recv_all(sock)
    finally:
        sock.close()
    header, separator, body = response.partition(b"\r\n\r\n")
    if not separator or not header.startswith(b"HTTP/1.1 200") or len(body) != 65536 or body != b"S" * 65536:
        raise AssertionError("invalid HTTP payload: header=%r bytes=%d" % (header[:120], len(body)))


def http_worker(stop, results, proxy, target, tls):
    name = "https" if tls else "http"
    while not stop.is_set():
        try:
            http_proxy_request(proxy, target, tls=tls)
            results.success(name)
        except Exception as error:
            results.failure(name, error)


def connect_tunnel(proxy, target):
    sock = socket.create_connection(proxy, timeout=5.0)
    sock.settimeout(5.0)
    host, port = target
    sock.sendall(
        ("CONNECT %s:%d HTTP/1.1\r\nHost: %s:%d\r\n\r\n" % (host, port, host, port)).encode()
    )
    header = recv_until(sock, b"\r\n\r\n")
    if not header.startswith(b"HTTP/1.1 200") and not header.startswith(b"HTTP/1.0 200"):
        sock.close()
        raise AssertionError("CONNECT failed: " + repr(header[:200]))
    return sock


def persistent_tcp_worker(stop, results, proxy, target):
    sock = None
    try:
        sock = connect_tunnel(proxy, target)
        sequence = 0
        while not stop.is_set():
            payload = ("smart14-persistent-tcp-%08d" % sequence).encode()
            sock.sendall(payload)
            if recv_exact(sock, len(payload)) != payload:
                raise AssertionError("persistent TCP payload mismatch")
            results.success("tcp_persistent")
            sequence += 1
            time.sleep(0.01)
    except Exception as error:
        results.failure("tcp_persistent", error)
    finally:
        if sock is not None:
            sock.close()


def new_tcp_worker(stop, results, proxy, target):
    sequence = 0
    while not stop.is_set():
        sock = None
        try:
            sock = connect_tunnel(proxy, target)
            payload = ("smart14-new-tcp-%08d" % sequence).encode()
            sock.sendall(payload)
            if recv_exact(sock, len(payload)) != payload:
                raise AssertionError("new TCP payload mismatch")
            results.success("tcp_new")
            sequence += 1
        except Exception as error:
            results.failure("tcp_new", error)
        finally:
            if sock is not None:
                sock.close()


def read_socks_address(sock):
    atyp = recv_exact(sock, 1)[0]
    if atyp == 1:
        host = socket.inet_ntoa(recv_exact(sock, 4))
    elif atyp == 3:
        host = recv_exact(sock, recv_exact(sock, 1)[0]).decode("ascii")
    elif atyp == 4:
        host = socket.inet_ntop(socket.AF_INET6, recv_exact(sock, 16))
    else:
        raise ValueError("unknown SOCKS address type %d" % atyp)
    return host, struct.unpack("!H", recv_exact(sock, 2))[0]


def socks_udp_associate(proxy):
    control = socket.create_connection(proxy, timeout=5.0)
    control.settimeout(5.0)
    control.sendall(b"\x05\x01\x00")
    if recv_exact(control, 2) != b"\x05\x00":
        raise AssertionError("SOCKS authentication failed")
    control.sendall(b"\x05\x03\x00\x01\x00\x00\x00\x00\x00\x00")
    header = recv_exact(control, 3)
    if header[:2] != b"\x05\x00":
        raise AssertionError("SOCKS UDP associate failed: " + repr(header))
    relay_host, relay_port = read_socks_address(control)
    if relay_host in ("0.0.0.0", "::"):
        relay_host = proxy[0]
    udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp.settimeout(5.0)
    return control, udp, (relay_host, relay_port)


def socks_udp_exchange(udp, relay, target, payload):
    host, port = target
    request = b"\x00\x00\x00\x01" + socket.inet_aton(host) + struct.pack("!H", port) + payload
    udp.sendto(request, relay)
    response, _ = udp.recvfrom(65535)
    if len(response) < 10 or response[:3] != b"\x00\x00\x00":
        raise AssertionError("invalid SOCKS UDP response")
    atyp = response[3]
    if atyp == 1:
        offset = 10
    elif atyp == 3:
        offset = 7 + response[4]
    elif atyp == 4:
        offset = 22
    else:
        raise AssertionError("invalid SOCKS UDP address type")
    return response[offset:]


def persistent_udp_worker(stop, results, proxy, target):
    control = udp = None
    try:
        control, udp, relay = socks_udp_associate(proxy)
        sequence = 0
        while not stop.is_set():
            payload = ("smart14-persistent-udp-%08d" % sequence).encode()
            if socks_udp_exchange(udp, relay, target, payload) != payload:
                raise AssertionError("persistent UDP payload mismatch")
            results.success("udp_persistent")
            sequence += 1
            time.sleep(0.01)
    except Exception as error:
        results.failure("udp_persistent", error)
    finally:
        if udp is not None:
            udp.close()
        if control is not None:
            control.close()


def new_udp_worker(stop, results, proxy, target):
    sequence = 0
    while not stop.is_set():
        control = udp = None
        try:
            control, udp, relay = socks_udp_associate(proxy)
            payload = ("smart14-new-udp-%08d" % sequence).encode()
            if socks_udp_exchange(udp, relay, target, payload) != payload:
                raise AssertionError("new UDP payload mismatch")
            results.success("udp_new")
            sequence += 1
        except Exception as error:
            results.failure("udp_new", error)
        finally:
            if udp is not None:
                udp.close()
            if control is not None:
                control.close()


def dns_query(query_id):
    name = b"\x07smart14\x03lab\x00"
    return struct.pack("!HHHHHH", query_id, 0x0100, 1, 0, 0, 0) + name + struct.pack("!HH", 1, 1)


def dns_worker(stop, results, proxy, target):
    control = udp = None
    try:
        control, udp, relay = socks_udp_associate(proxy)
        query_id = 1
        while not stop.is_set():
            packet = socks_udp_exchange(udp, relay, target, dns_query(query_id))
            if len(packet) < 16 or packet[:2] != struct.pack("!H", query_id) or packet[-4:] != socket.inet_aton("203.0.113.7"):
                raise AssertionError("invalid DNS response")
            results.success("dns")
            query_id = 1 if query_id == 65535 else query_id + 1
            time.sleep(0.01)
    except Exception as error:
        results.failure("dns", error)
    finally:
        if udp is not None:
            udp.close()
        if control is not None:
            control.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--api", default="http://10.254.40.117:19140")
    parser.add_argument("--proxy-host", default="10.254.40.117")
    parser.add_argument("--proxy-port", type=int, default=18140)
    parser.add_argument("--target-host", default="10.254.40.118")
    parser.add_argument("--duration", type=float, default=70.0)
    parser.add_argument("--result", required=True)
    args = parser.parse_args()

    proxy = (args.proxy_host, args.proxy_port)
    results = Results()
    stop = threading.Event()
    workers = [
        (proxy_state_worker, (stop, results, args.api, "SMART-HK", "hk-b", "♻️ 智能选择 · SMART-HK")),
        (proxy_state_worker, (stop, results, args.api, "SMART-US", "us-a", "♻️ 智能选择 · SMART-US")),
        (delay_worker, (stop, results, args.api, "♻️ 智能选择 · SMART-HK", "https://10.254.40.118:19443/generate_204")),
        (delay_worker, (stop, results, args.api, "♻️ 智能选择 · SMART-US", "https://10.254.40.118:19443/generate_204")),
        (http_worker, (stop, results, proxy, (args.target_host, 19080), False)),
        (http_worker, (stop, results, proxy, (args.target_host, 19443), True)),
        (persistent_tcp_worker, (stop, results, proxy, (args.target_host, 19081))),
        (new_tcp_worker, (stop, results, proxy, (args.target_host, 19081))),
        (persistent_udp_worker, (stop, results, proxy, (args.target_host, 19082))),
        (new_udp_worker, (stop, results, proxy, (args.target_host, 19082))),
        (dns_worker, (stop, results, proxy, (args.target_host, 19053))),
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
        "api_state_SMART-HK": 10,
        "api_state_SMART-US": 10,
        "delay_♻️ 智能选择 · SMART-HK": 10,
        "delay_♻️ 智能选择 · SMART-US": 10,
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
    error_counts = {key: value for key, value in results.counts.items() if key.endswith("_errors") and value}
    below_minimum = {key: minimum for key, minimum in minimums.items() if results.counts.get(key, 0) < minimum}
    summary["error_counts"] = error_counts
    summary["below_minimum"] = below_minimum
    summary["success"] = not error_counts and not below_minimum and summary["threads_alive"] == 0
    with open(args.result, "w", encoding="utf-8") as output:
        json.dump(summary, output, indent=2, sort_keys=True)
        output.write("\n")
    print(json.dumps(summary, sort_keys=True))


if __name__ == "__main__":
    main()
