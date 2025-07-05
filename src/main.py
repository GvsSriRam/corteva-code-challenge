#!/usr/bin/env python3
"""
Main script to run the complete enhanced weather data pipeline.
This script orchestrates data ingestion, analysis, and API startup with the optimal data model.
"""

import os
import sys
import logging
from datetime import datetime

# Add current directory to path for imports
sys.path.insert(0, os.path.dirname(__file__))

from models import create_engine_and_session, create_tables
from ingest import ingest_weather_data, ingest_yield_data, get_ingestion_summary
from analyze import calculate_all_aggregations, get_aggregation_summary

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def setup_database():
    """Initialize the database and create tables."""
    logger.info("Setting up enhanced database...")
    try:
        engine, _ = create_engine_and_session()
        create_tables(engine)
        logger.info("Enhanced database setup completed successfully")
        return True
    except Exception as e:
        logger.error(f"Database setup failed: {e}")
        return False

def run_enhanced_data_pipeline():
    """Run the complete enhanced data pipeline with optimal data model."""
    start_time = datetime.now()
    logger.info(f"Starting enhanced data pipeline at {start_time}")
    
    # Step 1: Setup database
    if not setup_database():
        logger.error("Pipeline failed at database setup")
        return False
    
    # Step 2: Ingest weather data (includes station creation and data quality tracking)
    logger.info("Step 1: Ingesting weather data with stations and quality tracking...")
    try:
        weather_records = ingest_weather_data()
        logger.info(f"Weather data ingestion completed: {weather_records} records")
    except Exception as e:
        logger.error(f"Weather data ingestion failed: {e}")
        return False
    
    # Step 3: Ingest yield data
    logger.info("Step 2: Ingesting yield data...")
    try:
        yield_records = ingest_yield_data()
        logger.info(f"Yield data ingestion completed: {yield_records} records")
    except Exception as e:
        logger.error(f"Yield data ingestion failed: {e}")
        return False
    
    # Step 4: Calculate comprehensive weather aggregations
    logger.info("Step 3: Calculating comprehensive weather aggregations...")
    try:
        aggregations_count = calculate_all_aggregations()
        logger.info(f"Weather aggregations calculation completed: {aggregations_count} records")
    except Exception as e:
        logger.error(f"Weather aggregations calculation failed: {e}")
        return False
    
    end_time = datetime.now()
    duration = end_time - start_time
    
    logger.info(f"Enhanced data pipeline completed successfully at {end_time}")
    logger.info(f"Total duration: {duration}")
    logger.info(f"Summary: {weather_records} weather records, {yield_records} yield records, {aggregations_count} aggregations")
    
    return True

def get_pipeline_summary():
    """Get comprehensive summary of the pipeline results."""
    logger.info("=== ENHANCED PIPELINE SUMMARY ===")
    
    # Get ingestion summary
    ingestion_summary = get_ingestion_summary()
    if ingestion_summary:
        logger.info("Ingestion Summary:")
        logger.info(f"  Stations: {ingestion_summary['stations']}")
        logger.info(f"  Weather Records: {ingestion_summary['weather_records']}")
        logger.info(f"  Yield Records: {ingestion_summary['yield_records']}")
        logger.info(f"  Quality Records: {ingestion_summary['quality_records']}")
        logger.info("  Data Quality Distribution:")
        for quality, count in ingestion_summary['quality_distribution'].items():
            logger.info(f"    {quality}: {count}")
    
    # Get aggregation summary
    aggregation_summary = get_aggregation_summary()
    if aggregation_summary:
        logger.info("Aggregation Summary:")
        logger.info(f"  Year Range: {aggregation_summary['year_range'][0]} - {aggregation_summary['year_range'][1]}")
        logger.info("  Aggregations by Type:")
        for period_type, count in aggregation_summary['aggregation_counts'].items():
            logger.info(f"    {period_type}: {count}")
    
    return {
        'ingestion': ingestion_summary,
        'aggregation': aggregation_summary
    }

def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Enhanced Weather Data Pipeline')
    parser.add_argument('--ingest-only', action='store_true', 
                       help='Only run data ingestion, skip analysis')
    parser.add_argument('--analyze-only', action='store_true',
                       help='Only run analysis, skip ingestion')
    parser.add_argument('--api-only', action='store_true',
                       help='Only start the API server')
    parser.add_argument('--port', type=int, default=5000,
                       help='Port for API server (default: 5000)')
    parser.add_argument('--summary', action='store_true',
                       help='Show pipeline summary only')
    
    args = parser.parse_args()
    
    if args.api_only:
        # Start API server only
        logger.info("Starting enhanced API server only...")
        from app import app
        app.run(debug=True, host='0.0.0.0', port=args.port)
        return
    
    if args.summary:
        # Show summary only
        logger.info("Showing pipeline summary...")
        summary = get_pipeline_summary()
        if summary:
            logger.info("Summary retrieved successfully")
        return
    
    if args.ingest_only:
        # Run ingestion only
        logger.info("Running enhanced data ingestion only...")
        setup_database()
        weather_records = ingest_weather_data()
        yield_records = ingest_yield_data()
        summary = get_ingestion_summary()
        logger.info(f"Enhanced ingestion complete: {weather_records} weather records, {yield_records} yield records")
        if summary:
            logger.info(f"Ingestion summary: {summary}")
        return
    
    if args.analyze_only:
        # Run analysis only
        logger.info("Running enhanced analysis only...")
        aggregations_count = calculate_all_aggregations()
        summary = get_aggregation_summary()
        logger.info(f"Enhanced analysis complete: {aggregations_count} aggregations")
        if summary:
            logger.info(f"Aggregation summary: {summary}")
        return
    
    # Run complete enhanced pipeline
    success = run_enhanced_data_pipeline()
    
    if success:
        logger.info("Enhanced pipeline completed successfully!")
        
        # Show comprehensive summary
        summary = get_pipeline_summary()
        
        logger.info("🎉 Enhanced Weather Data Pipeline & API is ready!")
        logger.info("\nNext steps:")
        logger.info("• Start the API server: python main.py --api-only")
        logger.info("• Visit API documentation: http://localhost:5000/docs")
        logger.info("• Check the logs for detailed information")
        logger.info("\nNew API endpoints available:")
        logger.info("• /api/stations - Weather station information")
        logger.info("• /api/weather - Enhanced weather records with quality")
        logger.info("• /api/weather/aggregations - Multiple time period aggregations")
        logger.info("• /api/quality - Data quality metrics")
        logger.info("• /api/yield - Enhanced yield data")
        logger.info("• /api/stats - System statistics")
    else:
        logger.error("Enhanced pipeline failed!")
        sys.exit(1)

if __name__ == "__main__":
    main() 