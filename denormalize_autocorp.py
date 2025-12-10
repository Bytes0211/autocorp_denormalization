#!/usr/bin/env python3
"""
AutoCorp Data Denormalization ETL Pipeline

This script orchestrates the denormalization of the AutoCorp PostgreSQL database
by creating denormalized fact tables and exporting them to CSV files.

Author: scotton
Date: December 10, 2025
"""

import os
import sys
import time
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import connection


# =============================================================================
# Configuration
# =============================================================================

# Database connection parameters
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'autocorp'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', '')
}

# Directory paths
PROJECT_DIR = Path(__file__).parent
SQL_DIR = PROJECT_DIR / 'sql'
OUTPUT_DIR = PROJECT_DIR / 'output'
LOG_DIR = PROJECT_DIR / 'logs'

# SQL scripts to execute (in order)
SQL_SCRIPTS = [
    '01_create_sales_order_fact.sql',
    '02_create_sales_order_line_items.sql',
    '03_create_service_parts_catalog.sql',
    '04_create_indexes.sql'
]

# Tables to export to CSV
EXPORT_TABLES = [
    'sales_order_fact',
    'sales_order_line_items',
    'service_parts_catalog'
]


# =============================================================================
# Logging Configuration
# =============================================================================

def setup_logging() -> logging.Logger:
    """Configure logging with timestamps and file output."""
    # Create logs directory if it doesn't exist
    LOG_DIR.mkdir(exist_ok=True)
    
    # Create log filename with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = LOG_DIR / f'denormalization_{timestamp}.log'
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    logger = logging.getLogger(__name__)
    logger.info(f"Logging initialized. Log file: {log_file}")
    return logger


# =============================================================================
# Database Connection
# =============================================================================

def get_db_connection() -> connection:
    """
    Establish connection to PostgreSQL database.
    
    Returns:
        psycopg2 connection object
    
    Raises:
        psycopg2.Error: If connection fails
    """
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False  # Use explicit transactions
        return conn
    except psycopg2.Error as e:
        logger.error(f"Failed to connect to database: {e}")
        raise


def close_db_connection(conn: connection) -> None:
    """Safely close database connection."""
    if conn:
        try:
            conn.close()
            logger.info("Database connection closed")
        except Exception as e:
            logger.error(f"Error closing database connection: {e}")


# =============================================================================
# SQL Execution
# =============================================================================

def execute_sql_file(conn: connection, sql_file: Path) -> Tuple[bool, float]:
    """
    Execute a SQL script file.
    
    Args:
        conn: Database connection
        sql_file: Path to SQL file
    
    Returns:
        Tuple of (success: bool, duration: float)
    """
    logger.info(f"Executing SQL script: {sql_file.name}")
    start_time = time.time()
    
    try:
        with open(sql_file, 'r') as f:
            sql_content = f.read()
        
        # Split SQL content by semicolons and execute each statement
        with conn.cursor() as cursor:
            # Remove psql-specific commands (\echo)
            sql_statements = []
            for line in sql_content.split('\n'):
                if not line.strip().startswith('\\'):
                    sql_statements.append(line)
            
            sql_clean = '\n'.join(sql_statements)
            cursor.execute(sql_clean)
            conn.commit()
        
        duration = time.time() - start_time
        logger.info(f"✓ Completed {sql_file.name} in {duration:.2f} seconds")
        return True, duration
    
    except Exception as e:
        conn.rollback()
        duration = time.time() - start_time
        logger.error(f"✗ Failed to execute {sql_file.name}: {e}")
        return False, duration


def run_denormalization_scripts(conn: connection) -> Dict[str, float]:
    """
    Execute all denormalization SQL scripts in sequence.
    
    Args:
        conn: Database connection
    
    Returns:
        Dictionary of script name to execution duration
    """
    logger.info("=" * 70)
    logger.info("Starting denormalization script execution")
    logger.info("=" * 70)
    
    execution_times = {}
    
    for script_name in SQL_SCRIPTS:
        script_path = SQL_DIR / script_name
        
        if not script_path.exists():
            logger.error(f"SQL script not found: {script_path}")
            continue
        
        success, duration = execute_sql_file(conn, script_path)
        execution_times[script_name] = duration
        
        if not success:
            logger.error(f"Stopping execution due to failure in {script_name}")
            break
    
    return execution_times


# =============================================================================
# Data Validation
# =============================================================================

def validate_table_row_counts(conn: connection) -> Dict[str, int]:
    """
    Validate row counts of denormalized tables.
    
    Args:
        conn: Database connection
    
    Returns:
        Dictionary of table name to row count
    """
    logger.info("=" * 70)
    logger.info("Validating denormalized table row counts")
    logger.info("=" * 70)
    
    row_counts = {}
    
    with conn.cursor() as cursor:
        for table_name in EXPORT_TABLES:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                count = cursor.fetchone()[0]
                row_counts[table_name] = count
                logger.info(f"✓ {table_name}: {count:,} rows")
            except Exception as e:
                logger.error(f"✗ Failed to count rows in {table_name}: {e}")
                row_counts[table_name] = -1
    
    return row_counts


def validate_financial_totals(conn: connection) -> bool:
    """
    Validate that financial totals match between source and denormalized tables.
    
    Args:
        conn: Database connection
    
    Returns:
        True if validation passes, False otherwise
    """
    logger.info("=" * 70)
    logger.info("Validating financial totals")
    logger.info("=" * 70)
    
    validation_query = """
    SELECT 
        'Source' as source,
        SUM(total_amount) as total_revenue
    FROM sales_order
    UNION ALL
    SELECT 
        'Denormalized',
        SUM(total_amount)
    FROM sales_order_fact
    """
    
    try:
        with conn.cursor() as cursor:
            cursor.execute(validation_query)
            results = cursor.fetchall()
            
            if len(results) == 2:
                source_total = float(results[0][1])
                denorm_total = float(results[1][1])
                
                logger.info(f"Source total revenue: ${source_total:,.2f}")
                logger.info(f"Denormalized total revenue: ${denorm_total:,.2f}")
                
                if abs(source_total - denorm_total) < 0.01:
                    logger.info("✓ Financial validation passed")
                    return True
                else:
                    logger.error("✗ Financial totals do not match!")
                    return False
    except Exception as e:
        logger.error(f"✗ Financial validation failed: {e}")
        return False
    
    return False


# =============================================================================
# CSV Export
# =============================================================================

def export_table_to_csv(conn: connection, table_name: str) -> Tuple[bool, str]:
    """
    Export a table to CSV using PostgreSQL COPY command.
    
    Args:
        conn: Database connection
        table_name: Name of table to export
    
    Returns:
        Tuple of (success: bool, csv_path: str)
    """
    # Create output directory if it doesn't exist
    OUTPUT_DIR.mkdir(exist_ok=True)
    
    # Generate CSV filename with timestamp
    timestamp = datetime.now().strftime('%Y%m%d')
    csv_filename = f"{table_name}_{timestamp}.csv"
    csv_path = OUTPUT_DIR / csv_filename
    
    logger.info(f"Exporting {table_name} to {csv_filename}")
    start_time = time.time()
    
    try:
        with conn.cursor() as cursor:
            with open(csv_path, 'w') as f:
                copy_sql = f"COPY {table_name} TO STDOUT WITH CSV HEADER"
                cursor.copy_expert(copy_sql, f)
        
        duration = time.time() - start_time
        file_size = csv_path.stat().st_size / (1024 * 1024)  # MB
        
        logger.info(f"✓ Exported {table_name}: {file_size:.2f} MB in {duration:.2f}s")
        return True, str(csv_path)
    
    except Exception as e:
        logger.error(f"✗ Failed to export {table_name}: {e}")
        return False, ""


def export_all_tables(conn: connection) -> Dict[str, str]:
    """
    Export all denormalized tables to CSV files.
    
    Args:
        conn: Database connection
    
    Returns:
        Dictionary of table name to CSV file path
    """
    logger.info("=" * 70)
    logger.info("Exporting denormalized tables to CSV")
    logger.info("=" * 70)
    
    export_results = {}
    
    for table_name in EXPORT_TABLES:
        success, csv_path = export_table_to_csv(conn, table_name)
        if success:
            export_results[table_name] = csv_path
    
    return export_results


# =============================================================================
# Main Execution
# =============================================================================

def main():
    """Main ETL pipeline execution."""
    global logger
    logger = setup_logging()
    
    logger.info("=" * 70)
    logger.info("AutoCorp Data Denormalization ETL Pipeline")
    logger.info("=" * 70)
    logger.info(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Track overall execution time
    pipeline_start = time.time()
    
    conn = None
    try:
        # Step 1: Connect to database
        logger.info("\nStep 1: Connecting to database...")
        conn = get_db_connection()
        logger.info(f"✓ Connected to {DB_CONFIG['database']} database")
        
        # Step 2: Execute denormalization scripts
        logger.info("\nStep 2: Executing denormalization SQL scripts...")
        execution_times = run_denormalization_scripts(conn)
        
        # Step 3: Validate data
        logger.info("\nStep 3: Validating denormalized data...")
        row_counts = validate_table_row_counts(conn)
        financial_valid = validate_financial_totals(conn)
        
        # Step 4: Export to CSV
        logger.info("\nStep 4: Exporting tables to CSV...")
        export_results = export_all_tables(conn)
        
        # Summary
        pipeline_duration = time.time() - pipeline_start
        
        logger.info("=" * 70)
        logger.info("Pipeline Execution Summary")
        logger.info("=" * 70)
        logger.info(f"Total execution time: {pipeline_duration:.2f} seconds")
        logger.info(f"SQL scripts executed: {len(execution_times)}")
        logger.info(f"Tables created: {len([c for c in row_counts.values() if c > 0])}")
        logger.info(f"CSV files exported: {len(export_results)}")
        logger.info(f"Financial validation: {'PASSED' if financial_valid else 'FAILED'}")
        
        logger.info("\nRow counts:")
        for table, count in row_counts.items():
            logger.info(f"  {table}: {count:,} rows")
        
        logger.info("\nExported CSV files:")
        for table, path in export_results.items():
            logger.info(f"  {path}")
        
        logger.info("=" * 70)
        logger.info("✓ Pipeline completed successfully!")
        logger.info("=" * 70)
        
        return 0
    
    except Exception as e:
        logger.error(f"✗ Pipeline failed with error: {e}", exc_info=True)
        return 1
    
    finally:
        if conn:
            close_db_connection(conn)


if __name__ == "__main__":
    sys.exit(main())
