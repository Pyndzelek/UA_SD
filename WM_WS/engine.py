import argparse
import socket
from protocol import recv_message, send_message
import threading

parser = argparse.ArgumentParser()
parser.add_argument("monitor_ip")    # 127.0.0.1
parser.add_argument("monitor_port", type=int)    # 6001
args = parser.parse_args()

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect((args.monitor_ip, args.monitor_port))
print(f"Connected to Monitor at {args.monitor_ip}:{args.monitor_port}")

state = {"ko": False}


def key_listener():
    while True:
        input("Press enter to toggle KO\n")
        state["ko"] = not state["ko"]
        print(f"Engine state is now: {'KO' if state['ko'] else 'OK'}")


threading.Thread(target=key_listener, daemon=True).start()

while True:
    message = recv_message(sock)
    if message == "HEALTH_CHECK":
        send_message(sock, "KO" if state["ko"] else "OK")

        
