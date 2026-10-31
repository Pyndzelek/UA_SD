import argparse
import socket
from protocol import recv_message, send_message
import threading
import time

parser = argparse.ArgumentParser()
parser.add_argument("monitor_ip")    # 127.0.0.1
parser.add_argument("monitor_port", type=int)    # 6001
parser.add_argument("kafka")    # localhost:9092
args = parser.parse_args()

state = {"ko": False, "watering": False, "remaining": 0, "volume": 0.0, "elapsed": 0, "stop": False,
         "blocked": False, "faulty": True, "request": 0}
FLOW = 12  # liters per minute


def menu():
    while True:
        print("1 - start watering   2 - stop watering   3 - toggle KO (simulate leak)")
        choice = input().strip()
        if choice == "1":
            if state["faulty"]:
                print("Faulty: monitor not reachable, watering not allowed")
            elif state["blocked"]:
                print("Out of Service: watering not allowed")
            elif state["ko"]:
                print("Leak detected: watering not allowed")
            elif state["watering"]:
                print("Already watering")
            else:
                seconds = input("How many seconds? ").strip()
                if not seconds.isdigit() or int(seconds) == 0:
                    print("Please type a number of seconds")
                    continue
                state["request"] = int(seconds)  # sent to the monitor on the next health check
                print("Watering requested, waiting for WM_Central to authorize it...")
        elif choice == "2":
            if state["watering"]:
                state["stop"] = True
            else:
                print("Not watering")
        elif choice == "3":
            state["ko"] = not state["ko"]
            print(f"Engine state is now: {'KO' if state['ko'] else 'OK'}")


def answer_health_check(sock):
    if state["ko"]:
        state["watering"] = False
        send_message(sock, "KO")
    elif state["watering"]:
        state["volume"] += FLOW / 60
        state["remaining"] -= 1
        state["elapsed"] += 1
        if state["remaining"] == 0 or state["stop"]:
            state["watering"] = False
            state["stop"] = False
            print(f"Watering finished: {state['volume']:.1f} l in {state['elapsed']} s")
            send_message(sock, f"DONE#{state['volume']:.1f}#{state['elapsed']}")
        else:
            send_message(sock, f"WATERING#{FLOW}#{state['volume']:.1f}#{state['elapsed']}")
    elif state["request"]:
        send_message(sock, f"REQUEST#{state['request']}")
        state["request"] = 0
    else:
        send_message(sock, "OK")


def serve(sock):
    while True:
        message = recv_message(sock)
        if message is None:
            continue

        fields = message.split("#")
        if fields[0] == "START":
            state["watering"] = True
            state["stop"] = False
            state["remaining"] = int(fields[1])
            state["volume"] = 0.0
            state["elapsed"] = 0
            print(f"Watering started for {fields[1]} seconds")
            send_message(sock, "STARTED")
        elif fields[0] == "DENIED":
            print(f"WM_Central denied the watering: {fields[1]}")
            send_message(sock, "OK")
        elif fields[0] == "BLOCK":
            state["watering"] = False
            state["blocked"] = True
            print("Out of Service")
            send_message(sock, "BLOCKED")
        elif fields[0] == "ACTIVATE":
            state["blocked"] = False
            print("Activated")
            send_message(sock, "ACTIVATED")
        elif fields[0] == "HEALTH_CHECK":
            answer_health_check(sock)


threading.Thread(target=menu, daemon=True).start()

while True:
    try:
        sock = socket.create_connection((args.monitor_ip, args.monitor_port))
    except OSError:
        print("Unable to connect to the Monitor, retrying in 3 s")
        time.sleep(3)
        continue

    print(f"Connected to Monitor at {args.monitor_ip}:{args.monitor_port}")
    sock.settimeout(5)  # the monitor checks every second, 5 s of silence = monitor down
    state["faulty"] = False
    try:
        serve(sock)
    except OSError:
        state["faulty"] = True
        state["request"] = 0
        if state["watering"]:
            state["watering"] = False
            print(f"Watering stopped after {state['volume']:.1f} l")
        print("Faulty: monitor not reachable")
        sock.close()
