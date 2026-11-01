# central on aws

panel: `http://<ec2 ip>:8000`

ssh: `ssh -i wm-key.pem ubuntu@<ec2 ip>`, then `cd watering_management_system`

logs: `docker logs -f watering_management_system-central-1`

admin console: `docker attach watering_management_system-central-1`

# lab

--- first station ---

terminal 1, monitor with its logs

`WS_ID=WS1 LOCATION=Park docker compose -p ws1 -f docker-compose.ws.yml up --build monitor`

terminal 2, engine with its menu

`docker compose -p ws1 -f docker-compose.ws.yml run --rm --build --no-deps engine`

--- second station ---

monitor
`WS_ID=WS2 LOCATION=Garden docker compose -p ws2 -f docker-compose.ws.yml up --build monitor`

engine
`docker compose -p ws2 -f docker-compose.ws.yml run --rm --build --no-deps engine`

--- field operators ---

`OPERATOR=OP1 docker compose -f docker-compose.fo.yml run --rm --build fo`

`OPERATOR=OP2 docker compose -f docker-compose.fo.yml run --rm --build fo`

field operator from the requests file

`docker compose -f docker-compose.fo.yml run --rm --build fo <ec2 ip>:9092 OP1 --file requests.txt`

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
