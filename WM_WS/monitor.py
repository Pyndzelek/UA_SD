import argparse
import os
import queue
import socket
import threading
import time
from protocol import send_message, recv_message

parser = argparse.ArgumentParser()
parser.add_argument("engine_port", type=int)  # 6001
parser.add_argument("central_ip") # 127.0.0.1
parser.add_argument("central_port", type=int) # 5000
parser.add_argument("ws_id") # id of this watering station
parser.add_argument("location", nargs="?", default="Park")
args = parser.parse_args()

# each socket belongs to one thread, the threads only talk through these
to_central = queue.Queue()  # LEAK, LEAK_CLEARED, DONE ... for central
to_engine = queue.Queue()   # START, BLOCK, ACTIVATE ... from central
state = {"leak": False, "progress": "", "last_progress": ""}


def every_second(function, *function_args):
    timer = threading.Timer(1, function, args=function_args)
    timer.daemon = True
    timer.start()


# ---------- engine side ----------

def report_leak(reason):
    if not state["leak"]:
        state["leak"] = True
        to_central.put(f"LEAK#{args.ws_id}")
        print(f"FAULT: {reason}, leak reported to central")


def health_check(conn, engine_lost):
    try:
        send_message(conn, "HEALTH_CHECK")
        answer = recv_message(conn)
        if answer is None:  # bad LRC, try again next second
            every_second(health_check, conn, engine_lost)
            return

        if answer == "KO":
            state["progress"] = ""
            report_leak("Engine reported KO")
        else:
            if state["leak"]:
                state["leak"] = False
                to_central.put(f"LEAK_CLEARED#{args.ws_id}")
                print("Engine OK again told central")
            print(f"Engine answered: {answer}")

            state["progress"] = ""
            if answer.startswith("WATERING"):
                state["progress"] = answer
                state["last_progress"] = answer
            elif answer.startswith("DONE"):
                # queued, so the summary is not lost if central is down
                fields = answer.split("#")
                to_central.put(f"DONE#{args.ws_id}#{fields[1]}#{fields[2]}")
                state["last_progress"] = ""

        while not to_engine.empty():
            command = to_engine.get()
            send_message(conn, command)
            print(f"Engine replied: {recv_message(conn)}")

    except OSError:  # timeout or engine closed the connection
        state["progress"] = ""
        report_leak("Engine not responding")
        engine_lost.set()
        return

    every_second(health_check, conn, engine_lost)


def engine_side():
    engine_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    engine_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    engine_server.bind(("0.0.0.0", args.engine_port))
    engine_server.listen()

    while True:
        print(f"Waiting for Engine on port {args.engine_port}")
        conn, addr = engine_server.accept()
        conn.settimeout(2)
        print(f"Engine connected from {addr}")

        engine_lost = threading.Event()
        health_check(conn, engine_lost)
        engine_lost.wait()
        conn.close()
        print("Engine lost")


# ---------- central side ----------

def report(sock, central_lost):
    try:
        while not to_central.empty():
            message = to_central.queue[0]  # only removed once central ACKs it
            if not send_message(sock, message):
                break
            to_central.get()

        poll = f"POLL#{args.ws_id}"
        if state["progress"]:
            poll += f"#{state['progress']}"
        send_message(sock, poll)
        command = recv_message(sock)
        if command and command != "NONE":
            print(f"Central ordered: {command}")
            to_engine.put(command)

    except OSError:
        print("Unable to connect to WM_Central, connection lost")
        central_lost.set()
        return

    every_second(report, sock, central_lost)


def central_side():
    while True:
        try:
            sock = socket.create_connection((args.central_ip, args.central_port), timeout=5)
            send_message(sock, f"REGISTER#{args.ws_id}#{args.location}")
            reply = recv_message(sock)
        except OSError:
            print("Unable to connect to WM_Central, retrying in 3 s")
            time.sleep(3)
            continue

        if reply is None or reply.startswith("REGISTER_DENIED"):
            print(f"Central refused the registration: {reply}")
            os._exit(1)
        print(f"Connected to WM_Central at {args.central_ip}:{args.central_port}: {reply}")

        if state["leak"]:  # always after connecting to central, it thinks we are REGISTER
            to_central.put(f"LEAK#{args.ws_id}")

        central_lost = threading.Event()
        report(sock, central_lost)
        central_lost.wait()
        sock.close()


engine_thread = threading.Thread(target=engine_side, daemon=True)
central_thread = threading.Thread(target=central_side, daemon=True)
engine_thread.start()
central_thread.start()
engine_thread.join()
