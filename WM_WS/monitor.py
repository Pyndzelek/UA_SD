import argparse
import socket
from protocol import build_frame, send_message, recv_message

parser = argparse.ArgumentParser()
parser.add_argument("central_ip") # 127.0.0.1
parser.add_argument("central_port", type=int) # 5000
parser.add_argument("ws_id") # id of this watering station
args = parser.parse_args()

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect((args.central_ip, args.central_port))
print(f"Connected to WM_Central at {args.central_ip}:{args.central_port}")

ack = send_message(sock, f"REGISTER#{args.ws_id}#Park")
print(f"Central ACKed my message: {ack}")

reply = recv_message(sock)
print(f"Central replied: {reply}")
