from sqlalchemy import (
    create_engine, Column, Integer, String, Date, SmallInteger, Float, DateTime, Boolean, Text, DECIMAL, Enum, ForeignKey, Index, CheckConstraint, Computed
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime
import os

Base = declarative_base()

# Check if we're using PostgreSQL to conditionally add the year column
DATABASE_URL = os.getenv('DATABASE_URL', 'sqlite:///weather_data.db')
IS_POSTGRES = DATABASE_URL.startswith('postgresql')

class Station(Base):
    """Weather station dimension table with metadata and spatial info."""
    __tablename__ = 'stations'
    
    station_id = Column(String(20), primary_key=True)
    name = Column(String(100), nullable=False)
    latitude = Column(DECIMAL(8,6), nullable=False)
    longitude = Column(DECIMAL(9,6), nullable=False)
    elevation = Column(DECIMAL(8,2))
    state = Column(String(2), nullable=False)
    country = Column(String(3), default='USA')
    timezone = Column(String(50), default='UTC')
    active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow)
    # For geospatial queries, add a GEOGRAPHY(Point) column if using PostGIS
    # location = Column(Geography('POINT'))  # Uncomment for PostGIS
    
    weather_facts = relationship("WeatherFact", back_populates="station", cascade="all, delete-orphan")
    
    __table_args__ = (
        Index('idx_location', 'latitude', 'longitude'),
        Index('idx_state', 'state'),
        Index('idx_active', 'active'),
    )

class WeatherFact(Base):
    """Fact table: one row per station, date, and (optionally) source. Stores raw and clean/generated values."""
    __tablename__ = 'weather_facts'
    
    # Composite PK for integrity & de-duplication
    station_id = Column(String(20), ForeignKey('stations.station_id', ondelete='CASCADE'), primary_key=True)
    observation_date = Column(Date, primary_key=True)
    source = Column(String(50), primary_key=True, default='manual')  # For multi-source ingestion
    
    # Raw values (storage efficiency: tenths of units, SMALLINT)
    raw_max_temp = Column(SmallInteger)  # tenths of deg C
    raw_min_temp = Column(SmallInteger)
    raw_precip = Column(SmallInteger)    # tenths of mm
    # Optionally: raw_humidity, raw_wind, etc.
    
    # Clean/generated columns (analytics friendliness)
    max_temp_c = Column(Float)           # deg C
    min_temp_c = Column(Float)
    precip_mm = Column(Float)
    precip_cm = Column(Float)
    # Add *_qc columns for QA'd values if needed
    
    # Data quality
    data_quality = Column(Enum('excellent', 'good', 'fair', 'poor', name='quality_enum'), default='good', index=True)
    quality_score = Column(DECIMAL(3,2), default=1.00, index=True)
    missing_values = Column(Integer, default=0)
    outlier_count = Column(Integer, default=0)
    quality_notes = Column(Text)
    
    # Lineage
    ingested_at = Column(DateTime, default=datetime.utcnow, index=True)
    ingest_run_id = Column(String(36), nullable=True)  # UUID for batch lineage

    # Relationships
    station = relationship("Station", back_populates="weather_facts")
    
    # Indexes for query speed
    __table_args__ = (
        # CHECK constraints for raw values (reasonable bounds or NULL)
        CheckConstraint('(raw_max_temp BETWEEN -9999 AND 6000 OR raw_max_temp IS NULL)', name='ck_raw_max_temp'),
        CheckConstraint('(raw_min_temp BETWEEN -9999 AND 6000 OR raw_min_temp IS NULL)', name='ck_raw_min_temp'),
        CheckConstraint('(raw_precip BETWEEN 0 AND 10000 OR raw_precip IS NULL)', name='ck_raw_precip'),
        Index('idx_obs_date', 'observation_date', postgresql_using='brin'),  # BRIN for Postgres, normal for SQLite
        Index('idx_station_date', 'station_id', 'observation_date'),
        Index('idx_quality', 'data_quality'),
    )
    # For materialized views: see analyze.py for annual stats

    # Optionally, drop raw_* columns after QA to save storage

# Only two main tables for simplicity: Station and WeatherFact
# All other analytics/aggregations can be materialized views or external tables

# Database setup

def get_database_url():
    return os.getenv('DATABASE_URL', 'sqlite:///weather_data.db')

def create_engine_and_session():
    engine = create_engine(get_database_url())
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    return engine, SessionLocal

def create_tables(engine):
    Base.metadata.create_all(bind=engine)

def get_db():
    engine, SessionLocal = create_engine_and_session()
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Helper functions for data validation
def validate_station_data(station_id, latitude, longitude, state):
    """Validate station data before insertion."""
    if not station_id or len(station_id) > 20:
        raise ValueError("Invalid station_id")
    if not (-90 <= float(latitude) <= 90):
        raise ValueError("Invalid latitude")
    if not (-180 <= float(longitude) <= 180):
        raise ValueError("Invalid longitude")
    if not state or len(state) != 2:
        raise ValueError("Invalid state code")
    return True

def calculate_data_quality_score(record):
    """Calculate data quality score for a weather record."""
    score = 1.0
    missing_count = 0
    
    # Check for missing values
    if record.max_temp is None:
        missing_count += 1
    if record.min_temp is None:
        missing_count += 1
    if record.precipitation is None:
        missing_count += 1
    
    # Reduce score based on missing data
    if missing_count > 0:
        score -= (missing_count * 0.2)
    
    # Check for logical inconsistencies
    if record.max_temp is not None and record.min_temp is not None:
        if record.max_temp < record.min_temp:
            score -= 0.3
    
    # Check for extreme values (potential outliers)
    if record.max_temp is not None and (record.max_temp > 60 or record.max_temp < -60):
        score -= 0.1
    if record.min_temp is not None and (record.min_temp > 50 or record.min_temp < -70):
        score -= 0.1
    if record.precipitation is not None and record.precipitation < 0:
        score -= 0.2
    
    return max(0.0, score) 