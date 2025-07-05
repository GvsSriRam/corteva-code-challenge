import logging
from datetime import datetime, date
from sqlalchemy import func, extract
from models import create_engine_and_session, WeatherFact, Station

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('analysis.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def annual_weather_aggregation(session):
    """Materialized view: annual stats per station using clean/generated columns."""
    logger.info("Calculating annual weather aggregations (materialized view style)...")
    results = session.query(
        WeatherFact.station_id,
        extract('year', WeatherFact.observation_date).label('year'),
        func.avg(WeatherFact.max_temp_c).label('avg_max_temp_c'),
        func.avg(WeatherFact.min_temp_c).label('avg_min_temp_c'),
        func.sum(WeatherFact.precip_mm).label('total_precip_mm'),
        func.count().label('record_count'),
        func.avg(WeatherFact.quality_score).label('avg_quality_score')
    ).group_by(
        WeatherFact.station_id,
        extract('year', WeatherFact.observation_date)
    ).all()
    logger.info(f"Annual aggregations calculated for {len(results)} station-years.")
    return results

def monthly_weather_aggregation(session):
    """Materialized view: monthly stats per station using clean/generated columns."""
    logger.info("Calculating monthly weather aggregations (materialized view style)...")
    results = session.query(
        WeatherFact.station_id,
        extract('year', WeatherFact.observation_date).label('year'),
        extract('month', WeatherFact.observation_date).label('month'),
        func.avg(WeatherFact.max_temp_c).label('avg_max_temp_c'),
        func.avg(WeatherFact.min_temp_c).label('avg_min_temp_c'),
        func.sum(WeatherFact.precip_mm).label('total_precip_mm'),
        func.count().label('record_count'),
        func.avg(WeatherFact.quality_score).label('avg_quality_score')
    ).group_by(
        WeatherFact.station_id,
        extract('year', WeatherFact.observation_date),
        extract('month', WeatherFact.observation_date)
    ).all()
    logger.info(f"Monthly aggregations calculated for {len(results)} station-months.")
    return results

def quarterly_weather_aggregation(session):
    """Materialized view: quarterly stats per station using clean/generated columns."""
    logger.info("Calculating quarterly weather aggregations (materialized view style)...")
    quarter_case = func.floor((extract('month', WeatherFact.observation_date) - 1) / 3 + 1)
    results = session.query(
        WeatherFact.station_id,
        extract('year', WeatherFact.observation_date).label('year'),
        quarter_case.label('quarter'),
        func.avg(WeatherFact.max_temp_c).label('avg_max_temp_c'),
        func.avg(WeatherFact.min_temp_c).label('avg_min_temp_c'),
        func.sum(WeatherFact.precip_mm).label('total_precip_mm'),
        func.count().label('record_count'),
        func.avg(WeatherFact.quality_score).label('avg_quality_score')
    ).group_by(
        WeatherFact.station_id,
        extract('year', WeatherFact.observation_date),
        quarter_case
    ).all()
    logger.info(f"Quarterly aggregations calculated for {len(results)} station-quarters.")
    return results

def run_all_aggregations():
    """Run all materialized view aggregations and print sample results."""
    engine, SessionLocal = create_engine_and_session()
    session = SessionLocal()
    try:
        annual = annual_weather_aggregation(session)
        monthly = monthly_weather_aggregation(session)
        quarterly = quarterly_weather_aggregation(session)
        logger.info(f"Sample annual aggregation: {annual[:1]}")
        logger.info(f"Sample monthly aggregation: {monthly[:1]}")
        logger.info(f"Sample quarterly aggregation: {quarterly[:1]}")
    finally:
        session.close()

if __name__ == "__main__":
    run_all_aggregations() 