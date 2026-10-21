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

state = {"ko": False, "watering": False, "remaining": 0, "volume": 0.0}
FLOW = 12  # liters per minute


def key_listener():
    while True:
        input("Press enter to toggle KO\n")
        state["ko"] = not state["ko"]
        print(f"Engine state is now: {'KO' if state['ko'] else 'OK'}")


threading.Thread(target=key_listener, daemon=True).start()

while True:
    message = recv_message(sock)
    if message is None:
        continue

    fields = message.split("#")
    if fields[0] == "START":
        state["watering"] = True
        state["remaining"] = int(fields[1])
        state["volume"] = 0.0
        print(f"Watering started for {fields[1]} seconds")
        send_message(sock, "STARTED")
    elif fields[0] == "BLOCK":
        state["watering"] = False
        print("Out of Service")
        send_message(sock, "BLOCKED")
    elif fields[0] == "ACTIVATE":
        print("Activated")
        send_message(sock, "ACTIVATED")
    elif fields[0] == "HEALTH_CHECK":
        if state["ko"]:
            state["watering"] = False
            send_message(sock, "KO")
        elif state["watering"]:
            state["volume"] += FLOW / 60
            state["remaining"] -= 1
            if state["remaining"] == 0:
                state["watering"] = False
                send_message(sock, f"DONE#{state['volume']:.1f}")
            else:
                send_message(sock, f"WATERING#{FLOW}#{state['volume']:.1f}")
        else:
            send_message(sock, "OK")

        
