from flask import Flask, request, jsonify
from flask_restx import Api, Resource, fields
from flask_cors import CORS
from sqlalchemy import and_, or_, func
from datetime import datetime
from models import (
    create_engine_and_session, 
    Station, WeatherFact
)
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Flask app
app = Flask(__name__)
CORS(app)

# Initialize Flask-RESTX API
api = Api(
    app,
    version='3.0',
    title='Weather Data Warehouse API',
    description='API for best-practice weather data warehouse (integrity, storage, analytics, lineage)',
    doc='/docs'
)

# Namespaces
station_ns = api.namespace('api/stations', description='Weather station dimension')
fact_ns = api.namespace('api/weather', description='Weather fact table (raw + clean)')

# Models for Swagger
station_model = api.model('Station', {
    'station_id': fields.String(required=True),
    'name': fields.String(),
    'latitude': fields.Float(),
    'longitude': fields.Float(),
    'elevation': fields.Float(),
    'state': fields.String(),
    'country': fields.String(),
    'timezone': fields.String(),
    'active': fields.Boolean(),
    'created_at': fields.DateTime(),
    'updated_at': fields.DateTime(),
})

fact_model = api.model('WeatherFact', {
    'station_id': fields.String(required=True),
    'observation_date': fields.Date(required=True),
    'source': fields.String(),
    # Raw values (tenths)
    'raw_max_temp': fields.Integer(),
    'raw_min_temp': fields.Integer(),
    'raw_precip': fields.Integer(),
    # Clean/generated
    'max_temp_c': fields.Float(),
    'min_temp_c': fields.Float(),
    'precip_mm': fields.Float(),
    'precip_cm': fields.Float(),
    # Data quality
    'data_quality': fields.String(),
    'quality_score': fields.Float(),
    'missing_values': fields.Integer(),
    'outlier_count': fields.Integer(),
    'quality_notes': fields.String(),
    # Lineage
    'ingested_at': fields.DateTime(),
    'ingest_run_id': fields.String(),
    'year': fields.Integer(),
})

pagination_model = api.model('Pagination', {
    'page': fields.Integer(),
    'per_page': fields.Integer(),
    'total': fields.Integer(),
    'pages': fields.Integer(),
    'has_next': fields.Boolean(),
    'has_prev': fields.Boolean(),
})

station_response = api.model('StationResponse', {
    'data': fields.List(fields.Nested(station_model)),
    'pagination': fields.Nested(pagination_model)
})

fact_response = api.model('WeatherFactResponse', {
    'data': fields.List(fields.Nested(fact_model)),
    'pagination': fields.Nested(pagination_model)
})

def get_paginated_response(query, page, per_page):
    total = query.count()
    pages = (total + per_page - 1) // per_page
    records = query.offset((page - 1) * per_page).limit(per_page).all()
    return {
        'data': records,
        'pagination': {
            'page': page,
            'per_page': per_page,
            'total': total,
            'pages': pages,
            'has_next': page < pages,
            'has_prev': page > 1
        }
    }

@station_ns.route('/')
class StationList(Resource):
    @station_ns.doc('get_stations', params={
        'page': 'Page number (default: 1)',
        'per_page': 'Records per page (default: 50, max: 1000)',
        'state': 'Filter by state',
        'active': 'Filter by active status (true/false)',
        'country': 'Filter by country'
    })
    @station_ns.marshal_with(station_response)
    def get(self):
        page = request.args.get('page', 1, type=int)
        per_page = min(request.args.get('per_page', 50, type=int), 1000)
        state = request.args.get('state')
        active = request.args.get('active')
        country = request.args.get('country')
        if page < 1:
            page = 1
        engine, SessionLocal = create_engine_and_session()
        session = SessionLocal()
        try:
            query = session.query(Station)
            if state:
                query = query.filter(Station.state == state)
            if active is not None:
                active_bool = active.lower() == 'true'
                query = query.filter(Station.active == active_bool)
            if country:
                query = query.filter(Station.country == country)
            query = query.order_by(Station.station_id)
            response = get_paginated_response(query, page, per_page)
            response['data'] = [
                {
                    'station_id': r.station_id,
                    'name': r.name,
                    'latitude': float(r.latitude),
                    'longitude': float(r.longitude),
                    'elevation': float(r.elevation) if r.elevation else None,
                    'state': r.state,
                    'country': r.country,
                    'timezone': r.timezone,
                    'active': r.active,
                    'created_at': r.created_at.isoformat() if r.created_at else None,
                    'updated_at': r.updated_at.isoformat() if r.updated_at else None
                }
                for r in response['data']
            ]
            return response
        finally:
            session.close()

@fact_ns.route('/')
class WeatherFactList(Resource):
    @fact_ns.doc('get_weather_facts', params={
        'page': 'Page number (default: 1)',
        'per_page': 'Records per page (default: 50, max: 1000)',
        'station_id': 'Filter by station ID',
        'start_date': 'Start date (YYYY-MM-DD)',
        'end_date': 'End date (YYYY-MM-DD)',
        'source': 'Filter by source',
        'data_quality': 'Filter by data quality',
    })
    @fact_ns.marshal_with(fact_response)
    def get(self):
        page = request.args.get('page', 1, type=int)
        per_page = min(request.args.get('per_page', 50, type=int), 1000)
        station_id = request.args.get('station_id')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        source = request.args.get('source')
        data_quality = request.args.get('data_quality')
        if page < 1:
            page = 1
        engine, SessionLocal = create_engine_and_session()
        session = SessionLocal()
        try:
            query = session.query(WeatherFact)
            if station_id:
                query = query.filter(WeatherFact.station_id == station_id)
            if start_date:
                query = query.filter(WeatherFact.observation_date >= start_date)
            if end_date:
                query = query.filter(WeatherFact.observation_date <= end_date)
            if source:
                query = query.filter(WeatherFact.source == source)
            if data_quality:
                query = query.filter(WeatherFact.data_quality == data_quality)
            query = query.order_by(WeatherFact.observation_date.desc(), WeatherFact.station_id)
            response = get_paginated_response(query, page, per_page)
            response['data'] = [
                {
                    'station_id': r.station_id,
                    'observation_date': r.observation_date.isoformat(),
                    'source': r.source,
                    'raw_max_temp': r.raw_max_temp,
                    'raw_min_temp': r.raw_min_temp,
                    'raw_precip': r.raw_precip,
                    'max_temp_c': r.max_temp_c,
                    'min_temp_c': r.min_temp_c,
                    'precip_mm': r.precip_mm,
                    'precip_cm': r.precip_cm,
                    'data_quality': r.data_quality,
                    'quality_score': float(r.quality_score) if r.quality_score else None,
                    'missing_values': r.missing_values,
                    'outlier_count': r.outlier_count,
                    'quality_notes': r.quality_notes,
                    'ingested_at': r.ingested_at.isoformat() if r.ingested_at else None,
                    'ingest_run_id': r.ingest_run_id,
                    'year': getattr(r, 'year', r.observation_date.year if r.observation_date else None)
                }
                for r in response['data']
            ]
            return response
        finally:
            session.close()

@api.route('/api/health')
class HealthCheck(Resource):
    @api.doc('health_check')
    def get(self):
        return {'status': 'healthy', 'timestamp': datetime.utcnow().isoformat()}

if __name__ == '__main__':
    # Create tables if they don't exist
    engine, _ = create_engine_and_session()
    from models import create_tables
    create_tables(engine)
    
    # Run the Flask app
    app.run(debug=True, host='0.0.0.0', port=5000) 