#!/usr/bin/env python3

import argparse
import socket
import struct


def question_end(packet):
    offset = 12
    while offset < len(packet):
        length = packet[offset]
        offset += 1
        if length == 0:
            return offset + 4
        if length & 0xC0:
            return offset + 1 + 4
        offset += length
    raise ValueError("truncated DNS question")


def response_for(packet, address):
    if len(packet) < 12:
        raise ValueError("truncated DNS header")
    end = question_end(packet)
    query_id = packet[:2]
    question = packet[12:end]
    header = query_id + struct.pack("!HHHHH", 0x8180, 1, 1, 0, 0)
    answer = b"\xc0\x0c" + struct.pack("!HHIH", 1, 1, 30, 4) + socket.inet_aton(address)
    return header + question + answer


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--listen", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=19053)
    parser.add_argument("--address", default="203.0.113.7")
    args = parser.parse_args()

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((args.listen, args.port))
    while True:
        packet, peer = sock.recvfrom(4096)
        try:
            sock.sendto(response_for(packet, args.address), peer)
        except ValueError:
            continue


if __name__ == "__main__":
    main()
