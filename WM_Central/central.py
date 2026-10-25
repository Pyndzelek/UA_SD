import argparse
import socket
import threading
from protocol import recv_message, send_message
from database import init_db, load_stations, save_station
from kafka import KafkaConsumer, KafkaProducer

import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs
import sys
import time
from collections import deque

logs = deque(maxlen=2000)


class Tee:
    def __init__(self, real):
        self.real = real

    def write(self, text):
        self.real.write(text)
        if text.strip():
            logs.append(time.strftime("%H:%M:%S ") + text.rstrip())

    def flush(self):
        self.real.flush()


sys.stdout = Tee(sys.stdout)

parser = argparse.ArgumentParser(description="WM_Central")
parser.add_argument("port", type=int) 
parser.add_argument("kafka")  # localhost:9092
args = parser.parse_args()

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server.bind(("0.0.0.0", args.port))
server.listen()
print(f"WM_Central listening on port {args.port}")

stations = {}
pending = {}
operators = {}
producer = KafkaProducer(bootstrap_servers=args.kafka)


def send_to_operator(operator_id, status, detail):
    producer.send("fo-replies", f"{operator_id}#{status}#{detail}".encode())
    producer.flush()


def print_stations():
    print("--- stations ---")
    for ws_id, info in stations.items():
        print(f"{ws_id:6} {info['location']:10} {info['state']}")

init_db()
for ws_id, location in load_stations():
    stations[ws_id] = {"location": location, "state": "DISCONNECTED"}
print_stations()


def handle_monitor(conn, addr):
    print(f"Connection from {addr}")
    ws_id = None
    try:
        while True:
            message = recv_message(conn)
            if message is None:
                continue
            print(f"Received: {message}")

            fields = message.split("#")
            if fields[0] == "REGISTER":
                ws_id = fields[1]
                print(f"Station {ws_id} registered at {fields[2]}")
                send_message(conn, f"REGISTER_OK#{ws_id}")
                stations[ws_id] = {"location": fields[2], "state": "AVAILABLE"}
                save_station(ws_id, fields[2])
                print_stations()
            elif fields[1] not in stations:
                print(f"Ignoring {fields[0]} from unknown station {fields[1]}")
                if fields[0] == "POLL":
                    send_message(conn, "NONE")
            elif fields[0] == "LEAK":
                stations[fields[1]]["state"] = "LEAK"
                print_stations()
                operator_id = operators.pop(fields[1], None)
                if operator_id:
                    send_to_operator(operator_id, "ABORTED", "leak detected")
            elif fields[0] == "LEAK_CLEARED":
                if stations[fields[1]]["state"] == "LEAK":
                    stations[fields[1]]["state"] = "AVAILABLE"
                print_stations()
            elif fields[0] == "POLL":
                if len(fields) > 2:
                    report_progress(fields[1], fields[2:])
                send_message(conn, pending.pop(fields[1], "NONE"))

    except ConnectionError:
        print(f"{addr} disconnected")
        if ws_id is not None:
            stations[ws_id]["state"] = "DISCONNECTED"
            print_stations()
    finally:
        conn.close()


def listen_requests():
    consumer = KafkaConsumer(
        "fo-requests",
        bootstrap_servers=args.kafka,
        auto_offset_reset="earliest",
        group_id="central",
    )

    for message in consumer:
        text = message.value.decode()
        print(f"FO request: {text}")
        fields = text.split("#")
        operator_id = fields[1]

        if fields[0] == "LIST":
            available = [f"{ws} ({info['location']})" for ws, info in stations.items() if info["state"] == "AVAILABLE"]
            reply = f"{operator_id}#LIST#{', '.join(available) or 'none'}"
        else:
            ws_id = fields[2]
            if ws_id not in stations:
                reply = f"{operator_id}#DENIED#unknown station"
            elif stations[ws_id]["state"] != "AVAILABLE":
                reply = f"{operator_id}#DENIED#station is {stations[ws_id]['state']}"
            else:
                stations[ws_id]["state"] = "WATERING"
                pending[ws_id] = "START#10"
                operators[ws_id] = operator_id
                print_stations()
                reply = f"{operator_id}#OK#irrigation ordered"

        producer.send("fo-replies", reply.encode())
        producer.flush()


def report_progress(ws_id, data):
    operator_id = operators.get(ws_id)
    if data[0] == "WATERING":
        stations[ws_id]["flow"] = data[1]
        stations[ws_id]["volume"] = data[2]
        if operator_id:
            send_to_operator(operator_id, "WATERING", f"{data[1]} l/min, {data[2]} l")
    elif data[0] == "DONE":
        print(f"{ws_id}: finished, total {data[1]} l in {data[2]} s")
        if operator_id:
            send_to_operator(operator_id, "DONE", f"total {data[1]} l in {data[2]} s")
            del operators[ws_id]
        if stations[ws_id]["state"] == "WATERING":
            stations[ws_id]["state"] = "AVAILABLE"
            print_stations()


def apply_command(action, ws_id):
    state = stations[ws_id]["state"]
    if action == "start":
        if state != "AVAILABLE":
            print(f"{ws_id} cannot start: it is {state}")
            return
        stations[ws_id]["state"] = "WATERING"
        pending[ws_id] = "START#10"
    elif action == "block":
        if state == "DISCONNECTED":
            print(f"{ws_id} cannot be blocked: it is DISCONNECTED")
            return
        stations[ws_id]["state"] = "OUT_OF_SERVICE"
        pending[ws_id] = "BLOCK"
        operator_id = operators.pop(ws_id, None)
        if operator_id:
            send_to_operator(operator_id, "ABORTED", "station out of service")
    elif action == "activate":
        if state != "OUT_OF_SERVICE":
            print(f"{ws_id} cannot be activated: it is {state}")
            return
        stations[ws_id]["state"] = "AVAILABLE"
        pending[ws_id] = "ACTIVATE"
    print_stations()


def admin_console():
    while True:
        parts = input().split()
        if len(parts) != 2 or parts[0] not in ("start", "block", "activate"):
            print("usage: start|block|activate <ws_id|all>")
            continue
        action, target = parts
        targets = list(stations) if target == "all" else [target]
        for ws_id in targets:
            if ws_id in stations:
                apply_command(action, ws_id)
            else:
                print(f"unknown station {ws_id}")


threading.Thread(target=admin_console, daemon=True).start()
threading.Thread(target=listen_requests, daemon=True).start()

class Panel(BaseHTTPRequestHandler):
    def do_GET(self):
        url = urlparse(self.path) 
        if url.path == "/state":
            body = json.dumps(stations).encode()
            content_type = "application/json"
        elif url.path == "/logs":
            body = json.dumps(list(logs)).encode()
            content_type = "application/json"
        elif url.path == "/do":
            query = parse_qs(url.query) # {"action": ["block"], "ws": ["WS1"]}
            action = query["action"][0]
            target = query["ws"][0]
            targets = list(stations) if target == "all" else [target]
            for ws_id in targets:
                if ws_id in stations and action in ("start", "block", "activate"):
                    apply_command(action, ws_id)
            body = b"ok"
            content_type = "text/plain"
        else:
            body = open("index.html", "rb").read()
            content_type = "text/html"
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


threading.Thread(target=HTTPServer(("0.0.0.0", 8000), Panel).serve_forever, daemon=True).start()


while True:
    conn, addr = server.accept()
    thread = threading.Thread(target=handle_monitor, args=(conn, addr), daemon=True)
    thread.start()