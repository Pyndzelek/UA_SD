import sqlite3

DB_FILE = "central.db"


def init_db():
    try:
        with sqlite3.connect(DB_FILE) as db:
            db.execute("CREATE TABLE IF NOT EXISTS stations (ws_id TEXT PRIMARY KEY, location TEXT)")
            print("Database initialized")
    except:
        print("Error initializing database")



def save_station(ws_id, location):
    try:
        with sqlite3.connect(DB_FILE) as db:
            db.execute("INSERT OR REPLACE INTO stations (ws_id, location) VALUES (?, ?)", (ws_id, location))
    except:
        print("Error saving station")


def load_stations():
    try:
        with sqlite3.connect(DB_FILE) as db:
            return db.execute("SELECT ws_id, location FROM stations").fetchall()
    except:
        print("Error loading stations")
        return []