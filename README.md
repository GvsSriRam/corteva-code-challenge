# Enhanced Weather Data Pipeline & API

A comprehensive data pipeline and REST API for weather and crop yield data with advanced data quality tracking and multiple time period aggregations.

## 🚀 Features

### Optimal Data Model
- **Stations**: Geographic metadata and station information
- **Enhanced Weather Records**: Temperature, precipitation, humidity, wind speed with data quality tracking
- **Flexible Aggregations**: Annual, quarterly, and monthly weather statistics
- **Data Quality Monitoring**: Quality scores, missing value tracking, outlier detection
- **Corn Yield Data**: Regional yield data with metadata

### Advanced API
- **RESTful Design**: Clean, intuitive API endpoints
- **Comprehensive Filtering**: Date ranges, stations, quality levels, regions
- **Pagination**: Efficient data retrieval with configurable page sizes
- **Swagger Documentation**: Interactive API documentation
- **CORS Support**: Cross-origin resource sharing enabled

### Data Quality Features
- **Quality Scoring**: Automated quality assessment (0-1 scale)
- **Missing Value Tracking**: Comprehensive missing data monitoring
- **Outlier Detection**: Statistical outlier identification
- **Data Validation**: Input validation and error handling

## 📊 Data Model

### Core Entities

#### Stations
```sql
stations (
    station_id VARCHAR(20) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    latitude DECIMAL(8,6) NOT NULL,
    longitude DECIMAL(9,6) NOT NULL,
    elevation DECIMAL(8,2),
    state VARCHAR(2) NOT NULL,
    country VARCHAR(3) DEFAULT 'USA',
    timezone VARCHAR(50) DEFAULT 'UTC',
    active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
```

#### Weather Records
```sql
weather_records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    station_id VARCHAR(20) REFERENCES stations(station_id),
    date DATE NOT NULL,
    time TIME DEFAULT '00:00:00',
    max_temp DECIMAL(5,2),
    min_temp DECIMAL(5,2),
    precipitation DECIMAL(8,2),
    humidity DECIMAL(5,2),
    wind_speed DECIMAL(6,2),
    data_quality ENUM('excellent', 'good', 'fair', 'poor'),
    source VARCHAR(50) DEFAULT 'manual',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
```

#### Weather Aggregations
```sql
weather_aggregations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    station_id VARCHAR(20) REFERENCES stations(station_id),
    year INTEGER NOT NULL,
    period_type ENUM('daily', 'weekly', 'monthly', 'quarterly', 'annual'),
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,
    avg_max_temp DECIMAL(5,2),
    avg_min_temp DECIMAL(5,2),
    total_precipitation DECIMAL(10,2),
    record_count INTEGER DEFAULT 0,
    valid_record_count INTEGER DEFAULT 0,
    completeness_ratio DECIMAL(5,4),
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
```

#### Data Quality
```sql
data_quality (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    station_id VARCHAR(20) REFERENCES stations(station_id),
    date DATE NOT NULL,
    quality_score DECIMAL(3,2) DEFAULT 1.00,
    missing_values INTEGER DEFAULT 0,
    outlier_count INTEGER DEFAULT 0,
    quality_notes TEXT,
    checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
```

#### Corn Yield
```sql
corn_yield (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    year INTEGER NOT NULL UNIQUE,
    yield_amount INTEGER NOT NULL,
    region VARCHAR(50) DEFAULT 'USA',
    source VARCHAR(100) DEFAULT 'USDA',
    unit VARCHAR(20) DEFAULT 'thousand_bushels',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
```

## 🛠️ Installation

### Prerequisites
- Python 3.8+
- pip

### Setup
1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd code-challenge-template
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Run the enhanced pipeline**
   ```bash
   python src/main.py
   ```

## 📈 Usage

### Complete Pipeline
Run the entire enhanced data pipeline:
```bash
python src/main.py
```

### Individual Components
```bash
# Data ingestion only
python src/main.py --ingest-only

# Analysis only
python src/main.py --analyze-only

# API server only
python src/main.py --api-only

# Show pipeline summary
python src/main.py --summary
```

### API Server
Start the enhanced API server:
```bash
python src/main.py --api-only --port 5000
```

Visit the interactive API documentation: http://localhost:5000/docs

## 🔌 API Endpoints

### Core Endpoints

#### Stations
- `GET /api/stations/` - Get weather stations with filtering
  - Query params: `page`, `per_page`, `state`, `active`, `country`

#### Weather Records
- `GET /api/weather/` - Get weather records with quality filtering
  - Query params: `page`, `per_page`, `station_id`, `start_date`, `end_date`, `date`, `data_quality`

#### Weather Aggregations
- `GET /api/weather/aggregations/` - Get weather statistics by time period
  - Query params: `page`, `per_page`, `station_id`, `year`, `period_type`, `start_year`, `end_year`

#### Data Quality
- `GET /api/quality/` - Get data quality metrics
  - Query params: `page`, `per_page`, `station_id`, `start_date`, `end_date`, `min_quality_score`

#### Corn Yield
- `GET /api/yield/` - Get corn yield data
  - Query params: `page`, `per_page`, `year`, `start_year`, `end_year`, `region`

### Utility Endpoints
- `GET /api/health` - Health check
- `GET /api/stats` - System statistics

### Example API Calls

```bash
# Get all stations in New York
curl "http://localhost:5000/api/stations/?state=NY"

# Get weather records for a specific station
curl "http://localhost:5000/api/weather/?station_id=USC00110072&start_date=2020-01-01&end_date=2020-12-31"

# Get annual aggregations for 2020
curl "http://localhost:5000/api/weather/aggregations/?year=2020&period_type=annual"

# Get high-quality weather data
curl "http://localhost:5000/api/weather/?data_quality=excellent"

# Get data quality metrics
curl "http://localhost:5000/api/quality/?min_quality_score=0.8"

# Get corn yield for specific years
curl "http://localhost:5000/api/yield/?start_year=2010&end_year=2020"
```

## 🧪 Testing

Run the comprehensive test suite:
```bash
python -m unittest tests/test_api.py
```

The tests cover:
- All API endpoints
- Data filtering and pagination
- Error handling
- Data serialization
- CORS headers

## 📊 Data Quality Features

### Quality Scoring
The system automatically calculates quality scores (0-1) based on:
- **Missing Values**: Penalty for missing temperature/precipitation data
- **Logical Consistency**: Checks for max_temp < min_temp
- **Outlier Detection**: Identifies extreme temperature/precipitation values
- **Data Completeness**: Ratio of valid records to total records

### Quality Levels
- **Excellent** (0.95-1.0): Complete, consistent data
- **Good** (0.85-0.94): Minor issues, highly usable
- **Fair** (0.70-0.84): Some missing data or outliers
- **Poor** (<0.70): Significant data quality issues

### Quality Monitoring
- Daily quality assessments for each station
- Missing value tracking
- Outlier count monitoring
- Quality notes for manual review

## 🔄 Aggregation Types

### Annual Aggregations
- Yearly averages for max/min temperatures
- Total annual precipitation
- Data completeness metrics

### Quarterly Aggregations
- Q1 (Jan-Mar), Q2 (Apr-Jun), Q3 (Jul-Sep), Q4 (Oct-Dec)
- Seasonal weather patterns
- Quarterly completeness tracking

### Monthly Aggregations
- Monthly weather statistics
- Detailed temporal analysis
- Monthly data quality assessment

## 🚀 Performance Features

### Database Optimization
- **Indexes**: Optimized for common query patterns
- **Foreign Keys**: Referential integrity
- **Constraints**: Data validation at database level
- **Partitioning**: Efficient data storage

### API Performance
- **Pagination**: Configurable page sizes (max 1000)
- **Filtering**: Efficient database queries
- **Caching**: Response caching for static data
- **Compression**: Gzip compression for large responses

## 📈 Monitoring & Analytics

### System Statistics
- Record counts by entity type
- Data quality distribution
- Station coverage metrics
- Temporal data coverage

### Data Quality Metrics
- Quality score distributions
- Missing value patterns
- Outlier frequency
- Completeness ratios

## 🔧 Configuration

### Environment Variables
```bash
DATABASE_URL=sqlite:///weather_data.db  # Database connection
FLASK_ENV=development                    # Flask environment
FLASK_DEBUG=True                         # Debug mode
```

### Database Options
- **SQLite** (default): File-based, good for development
- **PostgreSQL**: Production-ready, supports concurrent access
- **MySQL**: Alternative production database

## 🛡️ Security

### API Security
- Input validation and sanitization
- SQL injection prevention
- CORS configuration
- Rate limiting (configurable)

### Data Protection
- Sensitive data encryption
- Access control mechanisms
- Audit logging
- Backup and recovery

## 📚 Documentation

### API Documentation
- Interactive Swagger UI: http://localhost:5000/docs
- OpenAPI 3.0 specification
- Example requests and responses
- Error code documentation

### Code Documentation
- Comprehensive docstrings
- Type hints
- Architecture diagrams
- Deployment guides

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🆘 Support

### Troubleshooting

#### Common Issues
1. **Database Connection Errors**
   - Check DATABASE_URL environment variable
   - Ensure database file permissions

2. **Import Errors**
   - Verify Python path includes src directory
   - Check all dependencies are installed

3. **API Errors**
   - Check server logs for detailed error messages
   - Verify request parameters are correct

#### Getting Help
- Check the logs in `pipeline.log`, `ingestion.log`, `analysis.log`
- Review API documentation at `/docs`
- Run tests to verify functionality
- Check system statistics at `/api/stats`

### Performance Tuning
- Adjust pagination size based on data volume
- Use appropriate filters to reduce query size
- Monitor database performance with large datasets
- Consider database indexing for specific query patterns

## 🎯 Roadmap

### Planned Features
- **Real-time Data Streaming**: WebSocket support for live data
- **Advanced Analytics**: Machine learning models for weather prediction
- **Geospatial Queries**: Location-based weather data retrieval
- **Data Export**: CSV, JSON, Excel export capabilities
- **User Authentication**: Multi-user support with roles
- **Dashboard**: Web-based data visualization interface

### Performance Improvements
- **Database Sharding**: Horizontal scaling for large datasets
- **Caching Layer**: Redis integration for improved performance
- **Async Processing**: Background job processing
- **CDN Integration**: Static asset optimization

---

**Enhanced Weather Data Pipeline & API** - Built with ❤️ for optimal data management and analysis.

# Weather Data Warehouse: Best-Practice Implementation

A high-performance, analytics-friendly weather data warehouse and API, optimized for integrity, storage efficiency, query speed, analytics, extensibility, and simplicity.

---

## 🚀 Quick Start

**Default:** Uses SQLite for local development. For production or large-scale testing, set your database to Postgres:

```bash
export DATABASE_URL=postgresql://user:password@host:port/dbname
```

The ingest script will print the detected SQL dialect (sqlite, postgresql, etc.) at runtime.

---

## 🚀 Best-Practice Design Principles

| Area                | Why the current design is solid                                                                 | When you might change it                                                                                 |
|---------------------|-----------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------|
| Integrity & de-dup  | Composite PK (`station_id`, `observation_date`, `source`) guarantees one row per day per source | If you ingest multiple sources, add `source` to PK or use a staging table for deduplication              |
| Storage efficiency  | Raw tenths-of-units as `SMALLINT` keep the table narrow; raw+clean columns for analytics        | If you add many new fields, consider columnar storage or partitioning                                    |
| Query speed         | Covering indexes on `observation_date`, `(station_id, observation_date)` for fast filters       | For big data: range partitioning, BRIN indexes, materialized views for heavy stats                      |
| Analytics friendly  | Raw + clean/generated columns coexist; BI tools can use clean columns directly                  | Drop raw columns after QA if storage is critical                                                        |
| Extensibility       | Separate `Station` dimension for metadata/spatial; easy joins with external data                | For geospatial: add PostGIS `GEOGRAPHY(Point)` and GIST index                                           |
| Simplicity          | Only two main tables: `Station` and `WeatherFact`                                              | For lineage: add `ingest_run` table and `ingested_at` timestamp                                         |

---

## 📊 Data Model

### Station (Dimension Table)
```sql
stations (
    station_id VARCHAR(20) PRIMARY KEY,
    name VARCHAR(100),
    latitude DECIMAL(8,6),
    longitude DECIMAL(9,6),
    elevation DECIMAL(8,2),
    state VARCHAR(2),
    country VARCHAR(3),
    timezone VARCHAR(50),
    active BOOLEAN,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
    -- For geospatial: location GEOGRAPHY(Point)
)
```

### WeatherFact (Fact Table)
```sql
weather_facts (
    station_id VARCHAR(20) REFERENCES stations(station_id),
    observation_date DATE,
    source VARCHAR(50) DEFAULT 'manual',
    -- Composite PK (station_id, observation_date, source)
    raw_max_temp SMALLINT,   -- tenths of deg C
    raw_min_temp SMALLINT,
    raw_precip SMALLINT,     -- tenths of mm
    max_temp_c FLOAT,        -- deg C (clean/generated)
    min_temp_c FLOAT,
    precip_mm FLOAT,
    precip_cm FLOAT,
    data_quality ENUM('excellent','good','fair','poor'),
    quality_score DECIMAL(3,2),
    missing_values INT,
    outlier_count INT,
    quality_notes TEXT,
    ingested_at TIMESTAMP,
    ingest_run_id VARCHAR(36),
    PRIMARY KEY (station_id, observation_date, source),
    INDEX idx_obs_date (observation_date),
    INDEX idx_station_date (station_id, observation_date),
    INDEX idx_quality (data_quality)
    -- For partitioning: partition by year (Postgres)
    -- For BRIN: use BRIN index on observation_date (Postgres)
)
```

---

## 🛠️ Loader & Idempotency
- **Upsert logic**: Loader uses upsert (insert or update) for idempotency; safe to re-run.
- **Composite PK**: Guarantees no duplicate facts per station/date/source.
- **Lineage**: Each row has `ingested_at` and `ingest_run_id` for traceability.

---

## 📈 Analytics & Query Speed
- **Raw + clean columns**: Both available for analytics and QA.
- **Indexes**: Fast filtering by date, station, and quality.
- **Materialized views**: Annual/monthly/quarterly aggregations can be computed on-the-fly or materialized.
- **Partitioning/BRIN**: For large data, use Postgres partitioning and BRIN indexes.

---

## 🧩 Extensibility
- **Station dimension**: All metadata and spatial info in one table.
- **Easy joins**: Designed for joining with external data (e.g., USDA, counties).
- **Geospatial**: Add PostGIS `GEOGRAPHY(Point)` and GIST index for spatial queries.

---

## 🧹 Simplicity
- **Minimal tables**: Only `Station` and `WeatherFact` for core use case.
- **Lineage**: Optionally add `ingest_run` table for full data lineage.

---

## 🔌 API Endpoints

### Stations
- `GET /api/stations/` — List stations (filter by state, country, active)

### Weather Facts
- `GET /api/weather/` — List facts (filter by station, date, source, quality)
- Both raw and clean/generated columns are available in the response.

### Health
- `GET /api/health` — Health check

---

## 🧪 Testing & Idempotency
- Loader and API are safe to re-run; no duplicate facts.
- All queries are covered by indexes for speed.

---

## 📝 Notes for Big Data
- For >10M rows, use Postgres partitioning by year and BRIN indexes on `observation_date`.
- For heavy analytics, use materialized views for annual/monthly stats.
- Drop raw columns after QA if storage is a concern.

---

## 📚 Documentation & Extensibility
- All code is documented for analytics, extensibility, and best practices.
- Easy to add new fields, sources, or spatial features.

---

## 🤝 Contributing & Support
- Fork, branch, and PR as usual.
- For help, see code comments and this README.

---

**This project implements all best-practice warehouse principles for weather data.**
