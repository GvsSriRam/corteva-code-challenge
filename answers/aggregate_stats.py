'''
Computes:
Average max temperature (°C)
Average min temperature (°C)
Total precipitation (cm)
Stores them in a new table weather_summary
'''
#  IMPORTS
import sqlite3
import logging
from datetime import datetime

DB_PATH = "../ctva_weather.db" # Path to the SQLite DB
LOG_PATH = "../logs/etl.log"   # Log file for operations

#  LOGGING
logging.basicConfig(
    filename=LOG_PATH,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# SUMMARY SETUP
CREATE_SUMMARY_SQL = """
CREATE TABLE IF NOT EXISTS weather_summary (
    station_id TEXT,
    year INTEGER,
    avg_max_temp_c REAL,
    avg_min_temp_c REAL,
    total_precip_cm REAL,
    PRIMARY KEY (station_id, year)
);
"""

INSERT_SUMMARY_SQL = """
INSERT OR REPLACE INTO weather_summary (
    station_id, year, avg_max_temp_c, avg_min_temp_c, total_precip_cm
)
SELECT
    station_id,
    strftime('%Y', date) AS year,
    AVG(max_temp_c) / 10.0 AS avg_max,
    AVG(min_temp_c) / 10.0 AS avg_min,
    SUM(precipitation_mm) / 100.0 AS total_precip
FROM weather
WHERE max_temp_c IS NOT NULL AND min_temp_c IS NOT NULL AND precipitation_mm IS NOT NULL
GROUP BY station_id, year;
"""

def main():
    start_time = datetime.now()
    logging.info("Starting weather summary aggregation...")

    conn = sqlite3.connect(DB_PATH)
    with conn:
        conn.execute(CREATE_SUMMARY_SQL)
        conn.execute(INSERT_SUMMARY_SQL)

    conn.close()
    end_time = datetime.now()

    logging.info("Weather summary aggregation complete.")
    logging.info(f"Duration: {end_time - start_time}")

if __name__ == "__main__":
    main()