'''
Monitors a watch directory (data/incoming/) for new .txt files
Cleans and ingests them into the database
Avoids duplicate entries
Moves processed files to an archive/ folder
Logs all activity
'''
#  IMPORTS
import os
import shutil
import sqlite3
import logging
from datetime import datetime

#  CONFIG 
WATCH_DIR = "../data/incoming"
ARCHIVE_DIR = "../data/archive"
# decouples input and output for automation
DB_PATH = "../ctva_weather.db"
LOG_PATH = "../logs/etl.log"

#  LOGGING 
logging.basicConfig(
    filename=LOG_PATH,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

#  DATABASE & SCHEMA 
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
    if len(parts) != 4: # prevents bad data from entering the database
        return None
    date_str, max_temp, min_temp, precip = parts
    try:
        return {
            "date": datetime.strptime(date_str, "%Y%m%d").date(),
            "max_temp": None if max_temp == "-9999" else int(max_temp),
            "min_temp": None if min_temp == "-9999" else int(min_temp),
            "precip": None if precip == "-9999" else int(precip)
        }
    except:
        return None

def process_file(filepath, conn): #line by line ingestion
    station_id = os.path.splitext(os.path.basename(filepath))[0]
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
    return station_id, count

def archive_file(filepath): # prevent reprocessing
    if not os.path.exists(ARCHIVE_DIR):
        os.makedirs(ARCHIVE_DIR)
    filename = os.path.basename(filepath)
    shutil.move(filepath, os.path.join(ARCHIVE_DIR, filename))

def main():
    start_time = datetime.now()
    logging.info("ETL process started...")

    conn = connect_db()
    init_db(conn)

    total_files = 0
    total_records = 0

    for file in os.listdir(WATCH_DIR):
        if file.endswith(".txt"):
            file_path = os.path.join(WATCH_DIR, file)
            try:
                station_id, count = process_file(file_path, conn)
                logging.info(f"Ingested {count} records from {file} (station: {station_id})")
                total_records += count
                archive_file(file_path)
                total_files += 1
            except Exception as e:
                logging.error(f"Failed to process {file}: {e}")

    conn.commit()
    conn.close()

    end_time = datetime.now()
    logging.info(f"ETL process complete. Files processed: {total_files}, Records ingested: {total_records}")
    logging.info(f"Duration: {end_time - start_time}")

if __name__ == "__main__":
    main()
