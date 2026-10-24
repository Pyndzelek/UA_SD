import argparse
import time
from kafka import KafkaConsumer, KafkaProducer

parser = argparse.ArgumentParser(description="WM_FO")
parser.add_argument("kafka")  # localhost:9092
parser.add_argument("operator_id")  # OP1
parser.add_argument("ws_id", nargs="?")  # WS1 (optional)
parser.add_argument("--file")  # requests.txt (optional)
parser.add_argument("--list", action="store_true")
args = parser.parse_args()

consumer = KafkaConsumer("fo-replies", bootstrap_servers=args.kafka)
consumer.poll(timeout_ms=2000)
producer = KafkaProducer(bootstrap_servers=args.kafka)


def request(ws_id):
    producer.send("fo-requests", f"REQUEST#{args.operator_id}#{ws_id}".encode())
    producer.flush()
    print(f"{args.operator_id} asked for {ws_id}, waiting for the answer...")

    for message in consumer:
        operator_id, status, detail = message.value.decode().split("#")
        if operator_id == args.operator_id:
            print(f"Central: {status} ({detail})")
            if status in ("DENIED", "DONE", "ABORTED"):
                break

def list_available():
    producer.send("fo-requests", f"LIST#{args.operator_id}".encode())
    producer.flush()
    for message in consumer:
        operator_id, status, detail = message.value.decode().split("#")
        if operator_id == args.operator_id and status == "LIST":
            print(f"Available: {detail}")
            break

if args.list:
    list_available()
    raise SystemExit
    
if args.file:
    with open(args.file) as f:
        ws_ids = [line.strip() for line in f if line.strip()]
else:
    ws_ids = [args.ws_id]

for i, ws_id in enumerate(ws_ids):
    if i > 0:
        time.sleep(4)
    request(ws_id)