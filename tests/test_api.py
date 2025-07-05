import unittest
import json
import os
import sys
from datetime import date

# Add src directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from models import create_engine_and_session, create_tables, Station, WeatherFact
from app import app

class TestWeatherWarehouseAPI(unittest.TestCase):
    """Test cases for the weather data API with optimal data model."""
    
    def setUp(self):
        """Set up test database and sample data."""
        # Remove the test database file before each test run
        try:
            os.remove('test.db')
        except FileNotFoundError:
            pass
        # Use file-based SQLite for testing
        os.environ['DATABASE_URL'] = 'sqlite:///test.db'
        
        # Create tables
        self.engine, self.SessionLocal = create_engine_and_session()
        create_tables(self.engine)
        
        # Create test app
        app.config['TESTING'] = True
        self.app = app.test_client()
        
        # Create sample data
        self.create_sample_data()
    
    def create_sample_data(self):
        """Create sample data for testing."""
        session = self.SessionLocal()
        
        try:
            # Create sample stations
            station = Station(
                station_id='TEST001',
                name='Test Station',
                latitude=40.0,
                longitude=-75.0,
                elevation=100.0,
                state='PA',
                country='USA',
                timezone='UTC',
                active=True
            )
            
            session.add(station)
            
            # Create sample weather facts
            fact1 = WeatherFact(
                station_id='TEST001',
                observation_date=date(2020, 1, 1),
                source='manual',
                raw_max_temp=100,
                raw_min_temp=10,
                raw_precip=5,
                max_temp_c=10.0,
                min_temp_c=1.0,
                precip_mm=0.5,
                precip_cm=0.05,
                data_quality='excellent',
                quality_score=1.0,
                missing_values=0,
                outlier_count=0,
                quality_notes='All good',
                ingest_run_id='run-1'
            )
            fact2 = WeatherFact(
                station_id='TEST001',
                observation_date=date(2020, 1, 2),
                source='manual',
                raw_max_temp=120,
                raw_min_temp=20,
                raw_precip=10,
                max_temp_c=12.0,
                min_temp_c=2.0,
                precip_mm=1.0,
                precip_cm=0.1,
                data_quality='good',
                quality_score=0.9,
                missing_values=0,
                outlier_count=0,
                quality_notes='Good',
                ingest_run_id='run-1'
            )
            
            session.add(fact1)
            session.add(fact2)
            
            session.commit()
            
        finally:
            session.close()
    
    def test_health_check(self):
        """Test health check endpoint."""
        response = self.app.get('/api/health')
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        self.assertIn('status', data)
        self.assertEqual(data['status'], 'healthy')
    
    def test_station_list(self):
        """Test stations endpoint."""
        response = self.app.get('/api/stations/')
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        
        self.assertIn('data', data)
        self.assertEqual(len(data['data']), 1)
        self.assertEqual(data['data'][0]['station_id'], 'TEST001')
    
    def test_weather_fact_list(self):
        """Test weather records endpoint."""
        response = self.app.get('/api/weather/')
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        
        self.assertIn('data', data)
        self.assertEqual(len(data['data']), 2)
        
        # Check raw and clean columns
        for fact in data['data']:
            self.assertIn('raw_max_temp', fact)
            self.assertIn('max_temp_c', fact)
            self.assertIn('year', fact)  # generated column
    
    def test_weather_fact_filtering(self):
        """Test weather filtering."""
        # By station
        response = self.app.get('/api/weather/?station_id=TEST001')
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        self.assertEqual(len(data['data']), 2)
        
        # By date
        response = self.app.get('/api/weather/?start_date=2020-01-02')
        data = json.loads(response.data)
        self.assertEqual(len(data['data']), 1)
        
        # By data_quality
        response = self.app.get('/api/weather/?data_quality=excellent')
        data = json.loads(response.data)
        self.assertEqual(len(data['data']), 1)
    
    def test_pagination(self):
        """Test pagination functionality."""
        # Test with page parameter
        response = self.app.get('/api/weather/?page=1&per_page=1')
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        
        self.assertEqual(len(data['data']), 1)
        self.assertEqual(data['pagination']['page'], 1)
        self.assertEqual(data['pagination']['per_page'], 1)
        self.assertTrue(data['pagination']['has_next'])
    
    def test_check_constraints(self):
        """Test check constraints."""
        # Try to insert out-of-bounds raw value
        session = self.SessionLocal()
        with self.assertRaises(Exception):
            session.add(WeatherFact(
                station_id='TEST001',
                observation_date=date(2020, 1, 3),
                source='manual',
                raw_max_temp=99999,  # out of bounds
                raw_min_temp=10,
                raw_precip=5,
                max_temp_c=10.0,
                min_temp_c=1.0,
                precip_mm=0.5,
                precip_cm=0.05,
                data_quality='excellent',
                quality_score=1.0,
                missing_values=0,
                outlier_count=0,
                quality_notes='All good',
                ingest_run_id='run-1'
            ))
            session.commit()
        session.close()

    @classmethod
    def tearDownClass(cls):
        # Remove the test database file after all tests
        import os
        try:
            os.remove('test.db')
        except FileNotFoundError:
            pass

if __name__ == '__main__':
    unittest.main() 