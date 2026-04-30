#!/usr/bin/env python3
"""
Unit tests for AutoCorp Data Denormalization ETL Pipeline

Tests cover:
1. get_db_connection - database connection with valid credentials
2. execute_sql_file - SQL script execution and error handling
3. validate_table_row_counts - row count reporting for denormalized tables
4. validate_financial_totals - financial totals validation
5. export_table_to_csv - CSV export functionality

Author: scotton
Date: December 10, 2025
"""

import unittest
from unittest.mock import Mock, MagicMock, patch, mock_open, call
import tempfile
import os
from pathlib import Path
from datetime import datetime
import psycopg2

# Import the module to test
import denormalize_autocorp


class TestGetDbConnection(unittest.TestCase):
    """Test cases for get_db_connection function."""
    
    @patch('denormalize_autocorp.logger', create=True)
    @patch('denormalize_autocorp.psycopg2.connect')
    def test_successful_connection_with_valid_credentials(self, mock_connect, mock_logger):
        """Test that get_db_connection successfully connects with valid credentials."""
        # Arrange
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        
        # Act
        result = denormalize_autocorp.get_db_connection()
        
        # Assert
        mock_connect.assert_called_once_with(**denormalize_autocorp.DB_CONFIG)
        self.assertEqual(result, mock_conn)
        self.assertFalse(result.autocommit)
    
    @patch('denormalize_autocorp.logger', create=True)
    @patch('denormalize_autocorp.psycopg2.connect')
    def test_connection_failure_raises_exception(self, mock_connect, mock_logger):
        """Test that connection failure raises psycopg2.Error."""
        # Arrange
        mock_connect.side_effect = psycopg2.OperationalError("Connection refused")
        
        # Act & Assert
        with self.assertRaises(psycopg2.OperationalError):
            denormalize_autocorp.get_db_connection()
        
        mock_logger.error.assert_called_once()
        self.assertIn("Failed to connect to database", mock_logger.error.call_args[0][0])
    
    @patch('denormalize_autocorp.logger', create=True)
    @patch('denormalize_autocorp.psycopg2.connect')
    def test_autocommit_disabled_by_default(self, mock_connect, mock_logger):
        """Test that autocommit is set to False for explicit transactions."""
        # Arrange
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        
        # Act
        result = denormalize_autocorp.get_db_connection()
        
        # Assert
        self.assertEqual(mock_conn.autocommit, False)


class TestExecuteSqlFile(unittest.TestCase):
    """Test cases for execute_sql_file function."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_conn = MagicMock()
        self.mock_cursor = MagicMock()
        self.mock_conn.cursor.return_value.__enter__ = Mock(return_value=self.mock_cursor)
        self.mock_conn.cursor.return_value.__exit__ = Mock(return_value=False)
        
    @patch('denormalize_autocorp.logger', create=True)
    def test_execute_sql_file_success(self, mock_logger):
        """Test that execute_sql_file correctly executes a SQL script."""
        # Arrange
        sql_content = "CREATE TABLE test (id INT);\nINSERT INTO test VALUES (1);"
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
            f.write(sql_content)
            sql_file_path = Path(f.name)
        
        try:
            # Act
            success, duration = denormalize_autocorp.execute_sql_file(self.mock_conn, sql_file_path)
            
            # Assert
            self.assertTrue(success)
            self.assertGreater(duration, 0)
            self.mock_cursor.execute.assert_called_once()
            self.mock_conn.commit.assert_called_once()
            self.mock_conn.rollback.assert_not_called()
            
            # Verify logging
            mock_logger.info.assert_any_call(f"Executing SQL script: {sql_file_path.name}")
            self.assertTrue(any("Completed" in str(call) for call in mock_logger.info.call_args_list))
        finally:
            os.unlink(sql_file_path)
    
    @patch('denormalize_autocorp.logger', create=True)
    def test_execute_sql_file_handles_psql_commands(self, mock_logger):
        """Test that execute_sql_file filters out psql-specific commands."""
        # Arrange
        sql_content = "\\echo 'Starting...'\nCREATE TABLE test (id INT);\n\\echo 'Done'"
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
            f.write(sql_content)
            sql_file_path = Path(f.name)
        
        try:
            # Act
            success, duration = denormalize_autocorp.execute_sql_file(self.mock_conn, sql_file_path)
            
            # Assert
            self.assertTrue(success)
            executed_sql = self.mock_cursor.execute.call_args[0][0]
            self.assertNotIn('\\echo', executed_sql)
            self.assertIn('CREATE TABLE test', executed_sql)
        finally:
            os.unlink(sql_file_path)
    
    @patch('denormalize_autocorp.logger', create=True)
    def test_execute_sql_file_handles_execution_errors(self, mock_logger):
        """Test that execute_sql_file handles execution errors and rolls back."""
        # Arrange
        sql_content = "INVALID SQL STATEMENT;"
        self.mock_cursor.execute.side_effect = psycopg2.ProgrammingError("syntax error")
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
            f.write(sql_content)
            sql_file_path = Path(f.name)
        
        try:
            # Act
            success, duration = denormalize_autocorp.execute_sql_file(self.mock_conn, sql_file_path)
            
            # Assert
            self.assertFalse(success)
            self.assertGreater(duration, 0)
            self.mock_conn.rollback.assert_called_once()
            self.mock_conn.commit.assert_not_called()
            
            # Verify error logging
            mock_logger.error.assert_called_once()
            self.assertIn("Failed to execute", mock_logger.error.call_args[0][0])
        finally:
            os.unlink(sql_file_path)
    
    @patch('denormalize_autocorp.logger', create=True)
    def test_execute_sql_file_file_not_found(self, mock_logger):
        """Test that execute_sql_file handles missing file errors."""
        # Arrange
        sql_file_path = Path("/nonexistent/file.sql")
        
        # Act
        success, duration = denormalize_autocorp.execute_sql_file(self.mock_conn, sql_file_path)
        
        # Assert
        self.assertFalse(success)
        self.mock_conn.commit.assert_not_called()
        mock_logger.error.assert_called_once()


class TestValidateTableRowCounts(unittest.TestCase):
    """Test cases for validate_table_row_counts function."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_conn = MagicMock()
        self.mock_cursor = MagicMock()
        self.mock_conn.cursor.return_value.__enter__ = Mock(return_value=self.mock_cursor)
        self.mock_conn.cursor.return_value.__exit__ = Mock(return_value=False)
    
    @patch('denormalize_autocorp.logger', create=True)
    @patch('denormalize_autocorp.EXPORT_TABLES', ['sales_order_fact', 'sales_order_line_items'])
    def test_validate_table_row_counts_accurate_reporting(self, mock_logger):
        """Test that validate_table_row_counts accurately reports row counts."""
        # Arrange
        expected_counts = {
            'sales_order_fact': 1000,
            'sales_order_line_items': 5000
        }
        
        # Mock fetchone to return different values for each table
        self.mock_cursor.fetchone.side_effect = [
            (1000,),  # sales_order_fact
            (5000,)   # sales_order_line_items
        ]
        
        # Act
        result = denormalize_autocorp.validate_table_row_counts(self.mock_conn)
        
        # Assert
        self.assertEqual(result, expected_counts)
        self.assertEqual(self.mock_cursor.execute.call_count, 2)
        
        # Verify correct SQL queries
        calls = self.mock_cursor.execute.call_args_list
        self.assertIn('sales_order_fact', calls[0][0][0])
        self.assertIn('sales_order_line_items', calls[1][0][0])
        
        # Verify logging
        self.assertTrue(any('1,000' in str(call) for call in mock_logger.info.call_args_list))
        self.assertTrue(any('5,000' in str(call) for call in mock_logger.info.call_args_list))
    
    @patch('denormalize_autocorp.logger', create=True)
    @patch('denormalize_autocorp.EXPORT_TABLES', ['test_table'])
    def test_validate_table_row_counts_handles_errors(self, mock_logger):
        """Test that validate_table_row_counts handles query errors gracefully."""
        # Arrange
        self.mock_cursor.execute.side_effect = psycopg2.ProgrammingError("relation does not exist")
        
        # Act
        result = denormalize_autocorp.validate_table_row_counts(self.mock_conn)
        
        # Assert
        self.assertEqual(result, {'test_table': -1})
        mock_logger.error.assert_called_once()
        self.assertIn("Failed to count rows", mock_logger.error.call_args[0][0])
    
    @patch('denormalize_autocorp.logger', create=True)
    @patch('denormalize_autocorp.EXPORT_TABLES', ['table1', 'table2', 'table3'])
    def test_validate_table_row_counts_empty_tables(self, mock_logger):
        """Test that validate_table_row_counts handles empty tables correctly."""
        # Arrange
        self.mock_cursor.fetchone.side_effect = [(0,), (0,), (0,)]
        
        # Act
        result = denormalize_autocorp.validate_table_row_counts(self.mock_conn)
        
        # Assert
        self.assertEqual(result, {'table1': 0, 'table2': 0, 'table3': 0})


class TestValidateFinancialTotals(unittest.TestCase):
    """Test cases for validate_financial_totals function."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_conn = MagicMock()
        self.mock_cursor = MagicMock()
        self.mock_conn.cursor.return_value.__enter__ = Mock(return_value=self.mock_cursor)
        self.mock_conn.cursor.return_value.__exit__ = Mock(return_value=False)
    
    @patch('denormalize_autocorp.logger', create=True)
    def test_validate_financial_totals_matching_totals(self, mock_logger):
        """Test that validate_financial_totals correctly identifies matching totals."""
        # Arrange
        # Mock query results: source and denormalized totals match
        self.mock_cursor.fetchall.return_value = [
            ('Source', 1000000.00),
            ('Denormalized', 1000000.00)
        ]
        
        # Act
        result = denormalize_autocorp.validate_financial_totals(self.mock_conn)
        
        # Assert
        self.assertTrue(result)
        self.mock_cursor.execute.assert_called_once()
        
        # Verify the query contains the expected elements
        query = self.mock_cursor.execute.call_args[0][0]
        self.assertIn('sales_order', query)
        self.assertIn('sales_order_fact', query)
        self.assertIn('total_amount', query)
        
        # Verify logging
        mock_logger.info.assert_any_call("Source total revenue: $1,000,000.00")
        mock_logger.info.assert_any_call("Denormalized total revenue: $1,000,000.00")
        self.assertTrue(any("validation passed" in str(call).lower() for call in mock_logger.info.call_args_list))
    
    @patch('denormalize_autocorp.logger', create=True)
    def test_validate_financial_totals_mismatching_totals(self, mock_logger):
        """Test that validate_financial_totals correctly identifies mismatching totals."""
        # Arrange
        # Mock query results: source and denormalized totals do not match
        self.mock_cursor.fetchall.return_value = [
            ('Source', 1000000.00),
            ('Denormalized', 950000.00)  # Missing $50,000
        ]
        
        # Act
        result = denormalize_autocorp.validate_financial_totals(self.mock_conn)
        
        # Assert
        self.assertFalse(result)
        
        # Verify error logging
        mock_logger.error.assert_called_once()
        self.assertIn("do not match", mock_logger.error.call_args[0][0])
    
    @patch('denormalize_autocorp.logger', create=True)
    def test_validate_financial_totals_within_tolerance(self, mock_logger):
        """Test that validate_financial_totals accepts differences within tolerance."""
        # Arrange
        # Mock query results: difference is less than $0.01 (tolerance)
        self.mock_cursor.fetchall.return_value = [
            ('Source', 1000000.005),
            ('Denormalized', 1000000.001)  # Difference: 0.004 (within 0.01 tolerance)
        ]
        
        # Act
        result = denormalize_autocorp.validate_financial_totals(self.mock_conn)
        
        # Assert
        self.assertTrue(result)
    
    @patch('denormalize_autocorp.logger', create=True)
    def test_validate_financial_totals_handles_query_errors(self, mock_logger):
        """Test that validate_financial_totals handles database query errors."""
        # Arrange
        self.mock_cursor.execute.side_effect = psycopg2.ProgrammingError("table does not exist")
        
        # Act
        result = denormalize_autocorp.validate_financial_totals(self.mock_conn)
        
        # Assert
        self.assertFalse(result)
        mock_logger.error.assert_called_once()
        self.assertIn("validation failed", mock_logger.error.call_args[0][0])
    
    @patch('denormalize_autocorp.logger', create=True)
    def test_validate_financial_totals_incomplete_results(self, mock_logger):
        """Test that validate_financial_totals handles incomplete query results."""
        # Arrange
        # Only one result instead of two
        self.mock_cursor.fetchall.return_value = [
            ('Source', 1000000.00)
        ]
        
        # Act
        result = denormalize_autocorp.validate_financial_totals(self.mock_conn)
        
        # Assert
        self.assertFalse(result)


class TestExportTableToCsv(unittest.TestCase):
    """Test cases for export_table_to_csv function."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_conn = MagicMock()
        self.mock_cursor = MagicMock()
        self.mock_conn.cursor.return_value.__enter__ = Mock(return_value=self.mock_cursor)
        self.mock_conn.cursor.return_value.__exit__ = Mock(return_value=False)
        
        # Create a temporary directory for CSV output
        self.test_output_dir = tempfile.mkdtemp()
        self.original_output_dir = denormalize_autocorp.OUTPUT_DIR
        denormalize_autocorp.OUTPUT_DIR = Path(self.test_output_dir)
    
    def tearDown(self):
        """Clean up test fixtures."""
        # Restore original output directory
        denormalize_autocorp.OUTPUT_DIR = self.original_output_dir
        
        # Clean up temporary files
        import shutil
        shutil.rmtree(self.test_output_dir, ignore_errors=True)
    
    @patch('denormalize_autocorp.logger', create=True)
    def test_export_table_to_csv_success(self, mock_logger):
        """Test that export_table_to_csv successfully exports table data."""
        # Arrange
        table_name = 'sales_order_fact'
        csv_data = "id,name,amount\n1,Product A,100.00\n2,Product B,200.00\n"
        
        def mock_copy_expert(sql, file_obj):
            """Mock copy_expert to write data to the file."""
            file_obj.write(csv_data)
        
        self.mock_cursor.copy_expert = mock_copy_expert
        
        # Act
        success, csv_path = denormalize_autocorp.export_table_to_csv(self.mock_conn, table_name)
        
        # Assert
        self.assertTrue(success)
        self.assertIsNotNone(csv_path)
        self.assertTrue(os.path.exists(csv_path))
        
        # Verify file contains expected data
        with open(csv_path, 'r') as f:
            content = f.read()
        self.assertEqual(content, csv_data)
        
        # Verify filename format
        self.assertIn(table_name, csv_path)
        self.assertIn(datetime.now().strftime('%Y%m%d'), csv_path)
        self.assertTrue(csv_path.endswith('.csv'))
        
        # Verify logging
        mock_logger.info.assert_any_call(f"Exporting {table_name} to {os.path.basename(csv_path)}")
        self.assertTrue(any("Exported" in str(call) for call in mock_logger.info.call_args_list))
    
    @patch('denormalize_autocorp.logger', create=True)
    def test_export_table_to_csv_uses_correct_copy_command(self, mock_logger):
        """Test that export_table_to_csv uses the correct COPY command."""
        # Arrange
        table_name = 'test_table'
        copy_calls = []
        
        def mock_copy_expert(sql, file_obj):
            copy_calls.append(sql)
            file_obj.write("header\ndata")
        
        self.mock_cursor.copy_expert = mock_copy_expert
        
        # Act
        denormalize_autocorp.export_table_to_csv(self.mock_conn, table_name)
        
        # Assert
        self.assertEqual(len(copy_calls), 1)
        copy_sql = copy_calls[0]
        self.assertIn('COPY', copy_sql)
        self.assertIn(table_name, copy_sql)
        self.assertIn('TO STDOUT', copy_sql)
        self.assertIn('CSV HEADER', copy_sql)
    
    @patch('denormalize_autocorp.logger', create=True)
    def test_export_table_to_csv_handles_export_errors(self, mock_logger):
        """Test that export_table_to_csv handles export errors gracefully."""
        # Arrange
        table_name = 'nonexistent_table'
        self.mock_cursor.copy_expert = Mock(side_effect=psycopg2.ProgrammingError("relation does not exist"))
        
        # Act
        success, csv_path = denormalize_autocorp.export_table_to_csv(self.mock_conn, table_name)
        
        # Assert
        self.assertFalse(success)
        self.assertEqual(csv_path, "")
        
        # Verify error logging
        mock_logger.error.assert_called_once()
        self.assertIn("Failed to export", mock_logger.error.call_args[0][0])
    
    @patch('denormalize_autocorp.logger', create=True)
    def test_export_table_to_csv_creates_output_directory(self, mock_logger):
        """Test that export_table_to_csv creates output directory if it doesn't exist."""
        # Arrange
        import shutil
        shutil.rmtree(self.test_output_dir)  # Remove the directory
        self.assertFalse(os.path.exists(self.test_output_dir))
        
        table_name = 'test_table'
        self.mock_cursor.copy_expert = Mock(side_effect=lambda sql, f: f.write("data"))
        
        # Act
        success, csv_path = denormalize_autocorp.export_table_to_csv(self.mock_conn, table_name)
        
        # Assert
        self.assertTrue(success)
        self.assertTrue(os.path.exists(self.test_output_dir))
    
    @patch('denormalize_autocorp.logger', create=True)
    def test_export_table_to_csv_large_file(self, mock_logger):
        """Test that export_table_to_csv handles large CSV files correctly."""
        # Arrange
        table_name = 'large_table'
        large_data = "id,data\n" + "\n".join([f"{i},data{i}" for i in range(10000)])
        
        def mock_copy_expert(sql, file_obj):
            file_obj.write(large_data)
        
        self.mock_cursor.copy_expert = mock_copy_expert
        
        # Act
        success, csv_path = denormalize_autocorp.export_table_to_csv(self.mock_conn, table_name)
        
        # Assert
        self.assertTrue(success)
        
        # Verify file size is logged
        file_size_mb = os.path.getsize(csv_path) / (1024 * 1024)
        self.assertGreater(file_size_mb, 0)


if __name__ == '__main__':
    unittest.main()
