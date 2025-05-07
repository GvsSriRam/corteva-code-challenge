'''
Performs a one-time bulk ingestion of historical weather data 
from a specified directory (data/wx_data/) into a SQLite database.
'''
#  IMPORTS
import os
import sqlite3
import logging
from datetime import datetime

#  CONFIG 
DATA_DIR = "../data/wx_data"
DB_PATH = "../ctva_weather.db"
LOG_PATH = "../logs/etl.log"

#  LOGGING
logging.basicConfig(
    filename=LOG_PATH,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

#  DATABASE SETUP 
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS weather (
    station_id TEXT NOT NULL,
    date DATE NOT NULL,
    max_temp_c INTEGER,
    min_temp_c INTEGER,
    precipitation_mm INTEGER,
    PRIMARY KEY (station_id, date)
);
"""

INSERT_SQL = """
INSERT OR IGNORE INTO weather (station_id, date, max_temp_c, min_temp_c, precipitation_mm)
VALUES (?, ?, ?, ?, ?);
"""

#  FUNCTIONS
def connect_db():
    return sqlite3.connect(DB_PATH)

def init_db(conn):
    with conn:
        conn.execute(CREATE_TABLE_SQL)

def parse_line(line):
    parts = line.strip().split("\t")
    if len(parts) != 4:
        return None
    date_str, max_temp, min_temp, precip = parts
    return {
        "date": datetime.strptime(date_str, "%Y%m%d").date(),
        "max_temp": None if max_temp == "-9999" else int(max_temp),
        "min_temp": None if min_temp == "-9999" else int(min_temp),
        "precip": None if precip == "-9999" else int(precip)
    }

def ingest_file(filepath, station_id, conn): 
    count = 0
    with open(filepath, "r") as file:
        for line in file:
            data = parse_line(line)
            if data:
                conn.execute(INSERT_SQL, (
                    station_id,
                    data["date"],
                    data["max_temp"],
                    data["min_temp"],
                    data["precip"]
                ))
                count += 1
    return count

def main():
    start_time = datetime.now()
    logging.info("Starting weather data ingestion...")

    conn = connect_db()
    init_db(conn)

    total_records = 0
    for filename in os.listdir(DATA_DIR):
        if not filename.endswith(".txt"):
            continue
        station_id = os.path.splitext(filename)[0]
        file_path = os.path.join(DATA_DIR, filename)
        count = ingest_file(file_path, station_id, conn)
        total_records += count
        logging.info(f"Ingested {count} records from {filename}")

    conn.commit()
    conn.close()

    end_time = datetime.now()
    logging.info(f"Weather data ingestion complete. Total records: {total_records}")
    logging.info(f"Duration: {end_time - start_time}")

if __name__ == "__main__":
    main()
