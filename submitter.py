import socket
import json
import argparse
import time


def send_json(conn, obj):
    s = json.dumps(obj, separators=(",", ":")) + "\n"
    conn.sendall(s.encode("utf-8"))


def recv_json(conn):
    buf = b""
    while True:
        chunk = conn.recv(4096)
        if not chunk:
            return None
        buf += chunk
        if b"\n" in buf:
            line, rest = buf.split(b"\n", 1)
            return json.loads(line.decode("utf-8"))


def main(host, port):
    apps = [
        {"name": "Alex", "experience": 7},
        {"name": "Robby", "experience": 6},
        {"name": "James", "experience": 2},
        {"name": "Tyler", "experience": 2},
    ]
    with socket.create_connection((host, port)) as conn:
        send_json(conn, {"role": "submitter", "apps": apps})
        resp = recv_json(conn)
        print("Broker response:", resp)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("host")
    parser.add_argument("port", type=int)
    args = parser.parse_args()
    main(args.host, args.port)
