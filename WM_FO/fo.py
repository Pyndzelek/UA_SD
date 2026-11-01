import argparse
import time
from kafka import KafkaConsumer, KafkaProducer

parser = argparse.ArgumentParser(description="WM_FO")
parser.add_argument("kafka")  # localhost:9092
parser.add_argument("operator_id")  # OP1
parser.add_argument("ws_id", nargs="?")  # WS1 (optional, without it the menu is shown)
parser.add_argument("seconds", nargs="?", type=int, default=10)
parser.add_argument("--file")  # requests.txt (optional)
parser.add_argument("--list", action="store_true")
args = parser.parse_args()

try:
    consumer = KafkaConsumer(
        "fo-replies",
        bootstrap_servers=args.kafka,
        group_id=f"fo-{args.operator_id}",  # kafka remembers what this operator already read
        auto_commit_interval_ms=1000,
        session_timeout_ms=6000,  # a crashed FO is forgotten quickly, so a restart rejoins fast
        heartbeat_interval_ms=2000,
        consumer_timeout_ms=10000,  # stop waiting after 10 s without messages
    )
    producer = KafkaProducer(bootstrap_servers=args.kafka)
except Exception as error:
    print(f"Unable to connect to Kafka at {args.kafka}: {error}")
    raise SystemExit(1)


def show(message):
    operator_id, status, detail = message.value.decode().split("#", 2)
    if operator_id != args.operator_id:
        return None
    if status == "LIST":
        print(f"Available stations: {detail}")
    else:
        print(f"Central: {status} ({detail})")
    return status


def wait_for_answer(last_statuses):
    for message in consumer:
        if show(message) in last_statuses:
            return
    print("Unable to connect to WM_Central (no answer)")


def request(ws_id, seconds):
    producer.send("fo-requests", f"REQUEST#{args.operator_id}#{ws_id}#{seconds}".encode())
    producer.flush()
    print(f"{args.operator_id} asked for {ws_id} for {seconds} s, waiting for the answer...")
    wait_for_answer(("DENIED", "DONE", "ABORTED"))


def list_available():
    producer.send("fo-requests", f"LIST#{args.operator_id}".encode())
    producer.flush()
    wait_for_answer(("LIST",))


def run_file(path):
    # one watering per line: <station> [seconds], lines starting with # are skipped
    try:
        with open(path) as f:
            lines = [line.split() for line in f if line.strip() and not line.startswith("#")]
    except OSError:
        print(f"Cannot read {path}")
        return
    for i, fields in enumerate(lines):
        if i > 0:
            time.sleep(4)
        seconds = int(fields[1]) if len(fields) > 1 else 10
        request(fields[0], seconds)


def menu():
    while True:
        print("1 - list available stations   2 - request watering   3 - run requests from file   0 - quit")
        choice = input().strip()
        if choice == "1":
            list_available()
        elif choice == "2":
            ws_id = input("Station: ").strip()
            seconds = input("How many seconds? [10] ").strip() or "10"
            if not seconds.isdigit():
                print("Please type a number of seconds")
                continue
            request(ws_id, int(seconds))
        elif choice == "3":
            run_file(input("File: [requests.txt] ").strip() or "requests.txt")
        elif choice == "0":
            break


# join the group first and print the replies we missed while this FO was closed
print("Connecting to Kafka...")
while True:
    records = consumer.poll(timeout_ms=2000)
    for messages in records.values():
        for message in messages:
            show(message)
    if consumer.assignment() and not records:
        break

if args.list:
    list_available()
elif args.file:
    run_file(args.file)
elif args.ws_id:
    request(args.ws_id, args.seconds)
else:
    menu()

consumer.close()  # also saves what we read nothing is shown twice next time
