import os
import logging
from datetime import datetime, date
from sqlalchemy.orm import sessionmaker
from sqlalchemy import func
from sqlalchemy.dialects.sqlite import insert as sqlite_upsert
from sqlalchemy.dialects.postgresql import insert as pg_upsert
from sqlalchemy.exc import IntegrityError
from models import (
    create_engine_and_session, create_tables, 
    Station, WeatherFact
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('ingestion.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Station metadata mapping (in a real scenario, this would come from a separate file or API)
STATION_METADATA = {
    'USC00110072': {'name': 'Lincoln Municipal Airport', 'latitude': 40.8500, 'longitude': -96.7500, 'state': 'NE', 'elevation': 362.0},
    'USC00110187': {'name': 'Omaha Eppley Airfield', 'latitude': 41.3000, 'longitude': -95.9000, 'state': 'NE', 'elevation': 299.0},
    'USC00110338': {'name': 'Des Moines International Airport', 'latitude': 41.5333, 'longitude': -93.6500, 'state': 'IA', 'elevation': 294.0},
    'USC00111280': {'name': 'Cedar Rapids Municipal Airport', 'latitude': 41.8833, 'longitude': -91.7167, 'state': 'IA', 'elevation': 265.0},
    'USC00111436': {'name': 'Chicago O\'Hare International Airport', 'latitude': 41.9786, 'longitude': -87.9048, 'state': 'IL', 'elevation': 672.0},
    'USC00112140': {'name': 'Springfield Capital Airport', 'latitude': 39.8500, 'longitude': -89.6500, 'state': 'IL', 'elevation': 188.0},
    'USC00112193': {'name': 'Indianapolis International Airport', 'latitude': 39.7167, 'longitude': -86.2833, 'state': 'IN', 'elevation': 243.0},
    'USC00112348': {'name': 'Fort Wayne International Airport', 'latitude': 40.9833, 'longitude': -85.2000, 'state': 'IN', 'elevation': 247.0},
    'USC00112483': {'name': 'Cleveland Hopkins International Airport', 'latitude': 41.4117, 'longitude': -81.8497, 'state': 'OH', 'elevation': 791.0},
    'USC00113335': {'name': 'Cincinnati Northern Kentucky International Airport', 'latitude': 39.0500, 'longitude': -84.6667, 'state': 'OH', 'elevation': 273.0},
}

# Example: Ingest run ID for lineage (could be a UUID)
DEFAULT_INGEST_RUN_ID = 'default-run-001'

def create_stations_from_files(wx_data_dir, session):
    """Create station records from weather data files."""
    logger.info("Creating station records...")
    
    weather_files = [f for f in os.listdir(wx_data_dir) if f.endswith('.txt')]
    stations_created = 0
    
    for filename in weather_files:
        station_id = filename.replace('.txt', '')
        
        # Check if station already exists
        existing_station = session.query(Station).filter_by(station_id=station_id).first()
        if existing_station:
            logger.debug(f"Station {station_id} already exists")
            continue
        
        # Get metadata for this station
        metadata = STATION_METADATA.get(station_id, {
            'name': f'Weather Station {station_id}',
            'latitude': 0.0,  # Default values for unknown stations
            'longitude': 0.0,
            'state': 'XX',
            'elevation': 0.0
        })
        
        try:
            # Simple validation
            if not station_id or len(station_id) > 20:
                raise ValueError("Invalid station_id")
            if not (-90 <= metadata['latitude'] <= 90):
                raise ValueError("Invalid latitude")
            if not (-180 <= metadata['longitude'] <= 180):
                raise ValueError("Invalid longitude")
            if not metadata['state'] or len(metadata['state']) != 2:
                raise ValueError("Invalid state code")
            
            # Create station record
            station = Station(
                station_id=station_id,
                name=metadata['name'],
                latitude=metadata['latitude'],
                longitude=metadata['longitude'],
                elevation=metadata['elevation'],
                state=metadata['state'],
                country='USA',
                timezone='UTC',
                active=True
            )
            
            session.add(station)
            stations_created += 1
            logger.debug(f"Created station: {station_id} - {metadata['name']}")
            
        except ValueError as e:
            logger.warning(f"Invalid station data for {station_id}: {e}")
            continue
    
    session.commit()
    logger.info(f"Created {stations_created} new station records")
    return stations_created

def parse_weather_line(line):
    """Parse a single line from weather data file (raw tenths, clean values)."""
    try:
        parts = line.strip().split('\t')
        if len(parts) != 4:
            return None
        date_str, max_temp_str, min_temp_str, precip_str = parts
        observation_date = datetime.strptime(date_str, '%Y%m%d').date()
        # Raw values (tenths)
        raw_max_temp = int(max_temp_str) if max_temp_str != '-9999' else None
        raw_min_temp = int(min_temp_str) if min_temp_str != '-9999' else None
        raw_precip = int(precip_str) if precip_str != '-9999' else None
        # Clean/generated
        max_temp_c = raw_max_temp / 10.0 if raw_max_temp is not None else None
        min_temp_c = raw_min_temp / 10.0 if raw_min_temp is not None else None
        precip_mm = raw_precip / 10.0 if raw_precip is not None else None
        precip_cm = precip_mm / 10.0 if precip_mm is not None else None
        return {
            'observation_date': observation_date,
            'raw_max_temp': raw_max_temp,
            'raw_min_temp': raw_min_temp,
            'raw_precip': raw_precip,
            'max_temp_c': max_temp_c,
            'min_temp_c': min_temp_c,
            'precip_mm': precip_mm,
            'precip_cm': precip_cm
        }
    except Exception as e:
        logger.warning(f"Failed to parse line: {line.strip()}, error: {e}")
        return None



def upsert_weather_fact(session, fact_data):
    """Upsert (insert or update) a weather fact row for idempotency."""
    # Use SQLAlchemy's upsert for SQLite or Postgres
    table = WeatherFact.__table__
    dialect = session.bind.dialect.name
    if dialect == 'sqlite':
        upsert_stmt = sqlite_upsert(table).values(**fact_data)
        upsert_stmt = upsert_stmt.on_conflict_do_update(
            index_elements=['station_id', 'observation_date', 'source'],
            set_=fact_data
        )
    elif dialect == 'postgresql':
        upsert_stmt = pg_upsert(table).values(**fact_data)
        upsert_stmt = upsert_stmt.on_conflict_do_update(
            index_elements=['station_id', 'observation_date', 'source'],
            set_=fact_data
        )
    else:
        # Fallback: try/except for IntegrityError
        try:
            session.add(WeatherFact(**fact_data))
            session.commit()
            return
        except IntegrityError:
            session.rollback()
            session.query(WeatherFact).filter_by(
                station_id=fact_data['station_id'],
                observation_date=fact_data['observation_date'],
                source=fact_data['source']
            ).update(fact_data)
            session.commit()
            return
    session.execute(upsert_stmt)
    session.commit()

def ingest_weather_data(wx_data_dir='wx_data', source='manual', ingest_run_id=DEFAULT_INGEST_RUN_ID):
    """Idempotent ingestion of weather data into WeatherFact (composite PK, upsert)."""
    start_time = datetime.now()
    engine, SessionLocal = create_engine_and_session()
    create_tables(engine)
    session = SessionLocal()
    # Print detected SQL dialect
    dialect = session.bind.dialect.name
    logger.info(f"Detected SQL dialect: {dialect}")
    logger.info(f"Starting weather data ingestion at {start_time}")
    total_records = 0
    try:
        weather_files = [f for f in os.listdir(wx_data_dir) if f.endswith('.txt')]
        for filename in weather_files:
            station_id = filename.replace('.txt', '')
            file_path = os.path.join(wx_data_dir, filename)
            # Ensure station exists (minimal metadata for demo)
            if not session.query(Station).filter_by(station_id=station_id).first():
                session.add(Station(
                    station_id=station_id,
                    name=f"Station {station_id}",
                    latitude=0.0, longitude=0.0, state='XX', active=True
                ))
                session.commit()
            with open(file_path, 'r') as file:
                for line in file:
                    if line.strip():
                        parsed = parse_weather_line(line)
                        if parsed:
                            # Calculate data quality metrics
                            missing_values = sum(1 for value in [parsed['max_temp_c'], parsed['min_temp_c'], parsed['precip_mm']] if value is None)
                            outlier_count = 0
                            
                            # Check for outliers (simplified logic)
                            if parsed['max_temp_c'] is not None and (parsed['max_temp_c'] > 50 or parsed['max_temp_c'] < -50):
                                outlier_count += 1
                            if parsed['min_temp_c'] is not None and (parsed['min_temp_c'] > 40 or parsed['min_temp_c'] < -60):
                                outlier_count += 1
                            if parsed['precip_mm'] is not None and parsed['precip_mm'] > 1000:
                                outlier_count += 1
                            
                            # Calculate quality score
                            quality_score = max(0.0, 1.0 - (missing_values * 0.2) - (outlier_count * 0.1))
                            
                            # Check for logical inconsistencies
                            if (parsed['max_temp_c'] is not None and parsed['min_temp_c'] is not None and 
                                parsed['max_temp_c'] < parsed['min_temp_c']):
                                quality_score -= 0.3
                                quality_score = max(0.0, quality_score)
                            
                            # Determine data quality level
                            if quality_score >= 0.9:
                                data_quality = 'excellent'
                            elif quality_score >= 0.7:
                                data_quality = 'good'
                            elif quality_score >= 0.5:
                                data_quality = 'fair'
                            else:
                                data_quality = 'poor'
                            
                            fact_data = {
                                'station_id': station_id,
                                'observation_date': parsed['observation_date'],
                                'source': source,
                                'raw_max_temp': parsed['raw_max_temp'],
                                'raw_min_temp': parsed['raw_min_temp'],
                                'raw_precip': parsed['raw_precip'],
                                'max_temp_c': parsed['max_temp_c'],
                                'min_temp_c': parsed['min_temp_c'],
                                'precip_mm': parsed['precip_mm'],
                                'precip_cm': parsed['precip_cm'],
                                'data_quality': data_quality,
                                'quality_score': quality_score,
                                'missing_values': missing_values,
                                'outlier_count': outlier_count,
                                'quality_notes': f"Missing: {missing_values}, Outliers: {outlier_count}",
                                'ingested_at': datetime.utcnow(),
                                'ingest_run_id': ingest_run_id
                            }
                            upsert_weather_fact(session, fact_data)
                            total_records += 1
        logger.info(f"Weather data ingestion complete: {total_records} records")
    except Exception as e:
        logger.error(f"Error during ingestion: {e}")
        session.rollback()
        raise
    finally:
        session.close()
    return total_records



def get_ingestion_summary():
    """Get a summary of ingested data."""
    engine, SessionLocal = create_engine_and_session()
    session = SessionLocal()
    
    try:
        # Count records
        station_count = session.query(Station).count()
        weather_count = session.query(WeatherFact).count()
        
        # Get data quality distribution
        quality_distribution = session.query(
            WeatherFact.data_quality,
            func.count(WeatherFact.station_id)
        ).group_by(WeatherFact.data_quality).all()
        
        logger.info("=== INGESTION SUMMARY ===")
        logger.info(f"Stations: {station_count}")
        logger.info(f"Weather Facts: {weather_count}")
        logger.info("Data Quality Distribution:")
        for quality, count in quality_distribution:
            logger.info(f"  {quality}: {count}")
        
        return {
            'stations': station_count,
            'weather_facts': weather_count,
            'quality_distribution': dict(quality_distribution)
        }
        
    except Exception as e:
        logger.error(f"Error getting ingestion summary: {e}")
        return None
    finally:
        session.close()

if __name__ == "__main__":
    # Ingest weather data
    weather_records = ingest_weather_data()
    
    # Get summary
    summary = get_ingestion_summary()
    
    logger.info(f"Ingestion complete: {weather_records} weather records")
    if summary:
        logger.info(f"Summary: {summary}") 