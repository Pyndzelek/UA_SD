import argparse
import socket

from protocol import recv_message, send_message


parser = argparse.ArgumentParser(description="WM_Central")
parser.add_argument("port", type=int) 
args = parser.parse_args()

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server.bind(("0.0.0.0", args.port))
server.listen()
print(f"WM_Central listening on port {args.port}")

conn, addr = server.accept()   # blocks here until a Monitor connects
print(f"Connection from {addr}")

message = recv_message(conn)
print(f"Received: {message}")

fields = message.split("#")
if fields[0] == "REGISTER":
    ws_id = fields[1]
    print(f"Station {ws_id} registered at {fields[2]}")
    send_message(conn, f"REGISTER_OK#{ws_id}")