# lab

on both PCs: `cp .env.example .env` and put the ec2 public ip in it

PC 2, one station (repeat with another WS_ID and -p name for more stations)

`WS_ID=WS1 LOCATION=Park docker compose -p ws1 -f docker-compose.ws.yml up -d --build`

`WS_ID=WS2 LOCATION=Garden docker compose -p ws2 -f docker-compose.ws.yml up -d --build`

station menu: `docker attach ws1-engine-1` (leave with Ctrl+P Ctrl+Q)

monitor logs: `docker logs -f ws1-monitor-1`

crash a station: `docker kill ws1-engine-1` or `docker kill ws1-monitor-1`, start again: `docker compose -p ws1 -f docker-compose.ws.yml up -d`

PC 1, one field operator per terminal (menu)

`OPERATOR=OP1 docker compose -f docker-compose.fo.yml run --rm fo`

`OPERATOR=OP2 docker compose -f docker-compose.fo.yml run --rm fo`

field operator from the requests file (no interaction)

`docker compose -f docker-compose.fo.yml run --rm fo <ec2 ip>:9092 OP1 --file requests.txt`

requests.txt format: one watering per line, `<WS_ID> [seconds]`, default 10 seconds

# all on one computer with docker

`docker compose up --build`

field operator: `docker compose run --rm fo kafka:19092 OP1 WS1`

engine menu: `docker attach watering_management_system-engine-1`

# with python

central
`python3 WM_Central.py 5000 localhost:9092`

monitor
`python3 WM_WS_M.py 6001 localhost 5000 WS1 Park`

engine
`python3 WM_WS_E.py localhost:9092 localhost 6001`

fo
`python3 WM_FO.py localhost:9092 OP1` (menu)
`python3 WM_FO.py localhost:9092 OP1 WS1 15`
`python3 WM_FO.py localhost:9092 OP1 --file requests.txt`
