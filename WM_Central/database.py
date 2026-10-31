import sqlite3

DB_FILE = "central.db"


def init_db(db_file):
    global DB_FILE
    DB_FILE = db_file
    try:
        with sqlite3.connect(DB_FILE) as db:
            db.execute("CREATE TABLE IF NOT EXISTS stations (ws_id TEXT PRIMARY KEY, location TEXT)")
            db.execute("CREATE TABLE IF NOT EXISTS operators (op_id TEXT PRIMARY KEY)")
            print("Database initialized")
    except:
        print("Error initializing database")


def save_station(ws_id, location):
    try:
        with sqlite3.connect(DB_FILE) as db:
            db.execute("INSERT OR REPLACE INTO stations (ws_id, location) VALUES (?, ?)", (ws_id, location))
    except:
        print("Error saving station")


def save_operator(op_id):
    try:
        with sqlite3.connect(DB_FILE) as db:
            db.execute("INSERT OR IGNORE INTO operators (op_id) VALUES (?)", (op_id,))
    except:
        print("Error saving operator")


def load_stations():
    try:
        with sqlite3.connect(DB_FILE) as db:
            return db.execute("SELECT ws_id, location FROM stations").fetchall()
    except:
        print("Error loading stations")
        return []


def load_operators():
    try:
        with sqlite3.connect(DB_FILE) as db:
            return [row[0] for row in db.execute("SELECT op_id FROM operators").fetchall()]
    except:
        print("Error loading operators")
        return []
