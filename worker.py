import socket
import json
import time
import random
import sys
import argparse
from evaluator import evaluate_single


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


def worker_loop(host, port):

    conn = socket.create_connection((host, port))

    send_json(conn, {"role": "worker"})

    _ = recv_json(conn)

    try:
        while True:

            send_json(conn, {"cmd": "request"})
            resp = recv_json(conn)
            if resp is None:
                break
            task = resp.get("task")
            if not task:
                time.sleep(1.0)
                continue

            result = evaluate_single(task)
            send_json(conn, {"cmd": "result", "result": result})
            ack = recv_json(conn)
    except KeyboardInterrupt:
        send_json(conn, {"cmd": "quit"})
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("host")
    parser.add_argument("port", type=int)
    args = parser.parse_args()
    print("Worker connecting to broker", args.host, args.port)
    worker_loop(args.host, args.port)
