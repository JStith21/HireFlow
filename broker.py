import socket
import threading
import json
import argparse
from collections import deque

HOST = "0.0.0.0"
PORT = 6000

task_lock = threading.Lock()
tasks = deque()
results = []

printers = []
printers_lock = threading.Lock()
new_result_cond = threading.Condition()


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
            try:
                return json.loads(line.decode("utf-8"))
            except Exception:
                return None


def handle_submitter(conn, addr, init_msg):

    apps = init_msg.get("apps") or []
    with task_lock:
        for app in apps:
            tasks.append(app)

    send_json(conn, {"status": "OK", "added": len(apps)})
    conn.close()


def handle_worker(conn, addr, init_msg):

    send_json(conn, {"status": "CONNECTED", "msg": "worker protocol ready"})
    while True:
        try:
            msg = recv_json(conn)
        except:
            msg = None
        if not msg:
            break
        cmd = msg.get("cmd")
        if cmd == "request":
            with task_lock:
                if tasks:
                    task = tasks.popleft()
                    send_json(conn, {"task": task})
                else:
                    send_json(conn, {"task": None})
        elif cmd == "result":
            res = msg.get("result")
            if res is not None:
                with new_result_cond:
                    results.append(res)
                    new_result_cond.notify_all()
                send_json(conn, {"status": "RESULT_RECEIVED"})
            else:
                send_json(conn, {"status": "BAD_RESULT"})
        elif cmd == "quit":
            break
        else:
            send_json(conn, {"status": "UNKNOWN_CMD"})
    conn.close


def handle_printer(conn, addr, init_msg):

    send_json(conn, {"status": "CONNECTED", "history_len": len(results)})

    for r in results:
        send_json(conn, {"result": r})

    with printers_lock:
        printers.append(conn)
    try:
        while True:

            with new_result_cond:
                new_result_cond.wait()

                last = results[-1]
                send_json(conn, {"result": last})
    except Exception:
        pass
    finally:
        with printers_lock:
            if conn in printers:
                printers.remove(conn)
        conn.close()


def client_thread(conn, addr):
    try:
        init_msg = recv_json(conn)
        if not init_msg:
            conn.close()
            return
        role = init_msg.get("role")
        if role == "submitter":
            handle_submitter(conn, addr, init_msg)
        elif role == "worker":
            handle_worker(conn, addr, init_msg)
        elif role == "printer":
            handle_printer(conn, addr, init_msg)
        else:
            send_json(conn, {"status": "ERR", "msg": "Unknown role"})
            conn.close()
    except Exception as e:
        print(f"Connection error with {addr}: {e}")
        try:
            conn.close()
        except:
            pass


def main(host, port):
    print(f"Broker starting on {host}:{port}")
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind((host, port))
    sock.listen(100)
    try:
        while True:
            conn, addr = sock.accept()
            t = threading.Thread(target=client_thread, args=(conn, addr), daemon=True)
            t.start()
    except KeyboardInterrupt:
        print("Broker stopping...")
    finally:
        sock.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default=HOST)
    parser.add_argument("--port", type=int, default=PORT)
    args = parser.parse_args()
    main(args.host, args.port)
