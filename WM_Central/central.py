import argparse
import socket
import threading
from protocol import recv_message, send_message
from database import init_db, load_stations, save_station


parser = argparse.ArgumentParser(description="WM_Central")
parser.add_argument("port", type=int) 
args = parser.parse_args()

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server.bind(("0.0.0.0", args.port))
server.listen()
print(f"WM_Central listening on port {args.port}")

stations = {}


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
            elif fields[0] == "LEAK":
                stations[fields[1]]["state"] = "LEAK"
                print_stations()
            elif fields[0] == "LEAK_CLEARED":
                stations[fields[1]]["state"] = "AVAILABLE"
                print_stations()

    except ConnectionError:
        print(f"{addr} disconnected")
        if ws_id is not None:
            stations[ws_id]["state"] = "DISCONNECTED"
            print_stations()
    finally:
        conn.close()


while True:
    conn, addr = server.accept()
    thread = threading.Thread(target=handle_monitor, args=(conn, addr), daemon=True)
    thread.start()