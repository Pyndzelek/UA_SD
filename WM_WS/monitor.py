import argparse
import socket
from protocol import build_frame, send_message, recv_message
import time

parser = argparse.ArgumentParser()
parser.add_argument("engine_port", type=int)  # 6001
parser.add_argument("central_ip") # 127.0.0.1
parser.add_argument("central_port", type=int) # 5000
parser.add_argument("ws_id") # id of this watering station
parser.add_argument("location", nargs="?", default="Park")
args = parser.parse_args()

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect((args.central_ip, args.central_port))
print(f"Connected to WM_Central at {args.central_ip}:{args.central_port}")

ack = send_message(sock, f"REGISTER#{args.ws_id}#{args.location}")
print(f"Central ACKed my message: {ack}")

reply = recv_message(sock)
print(f"Central replied: {reply}")

engine_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
engine_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
engine_server.bind(("0.0.0.0", args.engine_port))
engine_server.listen()
print(f"Waiting for Engine on port {args.engine_port}")

engine_conn, engine_addr = engine_server.accept()
print(f"Engine connected from {engine_addr}")

leak = False

while True:
    send_message(engine_conn, "HEALTH_CHECK")
    answer = recv_message(engine_conn)
    progress = ""
    if answer and answer.startswith(("WATERING", "DONE")):
        progress = answer
    if answer == "KO":
        if not leak:
            leak = True
            send_message(sock, f"LEAK#{args.ws_id}")
            print("FAULT: Engine reported KO and leak reported to central")
    else:
        if leak:
            leak = False
            send_message(sock, f"LEAK_CLEARED#{args.ws_id}")
            print("Engine OK again told central")
        print(f"Engine answered: {answer}")
    poll = f"POLL#{args.ws_id}"
    if progress:
        poll += f"#{progress}"
    send_message(sock, poll)
    command = recv_message(sock)
    if command != "NONE":
        print(f"Central ordered: {command}")
        send_message(engine_conn, command)
        print(f"Engine replied: {recv_message(engine_conn)}")

    time.sleep(1)

sock.close()
print("Disconnected from WM_Central")