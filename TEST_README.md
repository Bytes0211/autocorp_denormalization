# Unit Tests for AutoCorp Denormalization ETL Pipeline

## Overview

This document describes the unit tests for the AutoCorp Data Denormalization ETL Pipeline. The tests cover the five main functions of the pipeline with comprehensive test cases.

## Test Coverage

The test suite (`test_denormalize_autocorp.py`) includes 20 unit tests covering:

### 1. `get_db_connection` (3 tests)
- Successfully connects to the database with valid credentials
- Handles connection failures and raises appropriate exceptions
- Ensures autocommit is disabled for explicit transaction control

### 2. `execute_sql_file` (4 tests)
- Correctly executes SQL scripts and commits transactions
- Filters out psql-specific commands (e.g., `\echo`)
- Handles execution errors and performs rollback
- Handles missing SQL file errors gracefully

### 3. `validate_table_row_counts` (3 tests)
- Accurately reports row counts for denormalized tables
- Handles database query errors and reports -1 for failed counts
- Correctly handles empty tables with 0 rows

### 4. `validate_financial_totals` (5 tests)
- Correctly identifies matching financial totals between source and denormalized data
- Correctly identifies mismatching financial totals
- Accepts differences within tolerance (< $0.01)
- Handles database query errors gracefully
- Handles incomplete query results

### 5. `export_table_to_csv` (5 tests)
- Successfully exports table data to CSV files
- Uses the correct PostgreSQL COPY command with CSV headers
- Handles export errors gracefully
- Creates output directory if it doesn't exist
- Correctly handles large CSV files

## Running the Tests

### Prerequisites

Ensure you have the project dependencies installed:

```bash
# Activate the virtual environment
source .denormVenv/bin/activate

# Or use the virtual environment Python directly
.denormVenv/bin/python
```

### Run All Tests

```bash
# Using unittest (verbose mode)
python -m unittest test_denormalize_autocorp -v

# Or with the virtual environment Python
.denormVenv/bin/python -m unittest test_denormalize_autocorp -v
```

### Run Specific Test Classes

```bash
# Run only get_db_connection tests
python -m unittest test_denormalize_autocorp.TestGetDbConnection -v

# Run only execute_sql_file tests
python -m unittest test_denormalize_autocorp.TestExecuteSqlFile -v

# Run only validate_table_row_counts tests
python -m unittest test_denormalize_autocorp.TestValidateTableRowCounts -v

# Run only validate_financial_totals tests
python -m unittest test_denormalize_autocorp.TestValidateFinancialTotals -v

# Run only export_table_to_csv tests
python -m unittest test_denormalize_autocorp.TestExportTableToCsv -v
```

### Run Individual Tests

```bash
# Example: run a specific test
python -m unittest test_denormalize_autocorp.TestGetDbConnection.test_successful_connection_with_valid_credentials -v
```

## Test Design

The tests use Python's built-in `unittest` framework with extensive mocking to avoid actual database connections. Key testing approaches include:

- **Mocking**: Uses `unittest.mock` to mock database connections, cursors, and file I/O operations
- **Isolation**: Each test is independent and doesn't rely on actual database state
- **Fixtures**: Uses `setUp()` and `tearDown()` methods for test initialization and cleanup
- **Temporary Files**: Creates temporary files and directories for testing file operations
- **Error Simulation**: Tests error handling by raising exceptions in mocked methods

## Test Results

When all tests pass, you should see output similar to:

```
test_autocommit_disabled_by_default ... ok
test_connection_failure_raises_exception ... ok
test_successful_connection_with_valid_credentials ... ok
test_execute_sql_file_file_not_found ... ok
test_execute_sql_file_handles_execution_errors ... ok
test_execute_sql_file_handles_psql_commands ... ok
test_execute_sql_file_success ... ok
test_export_table_to_csv_creates_output_directory ... ok
test_export_table_to_csv_handles_export_errors ... ok
test_export_table_to_csv_large_file ... ok
test_export_table_to_csv_success ... ok
test_export_table_to_csv_uses_correct_copy_command ... ok
test_validate_financial_totals_handles_query_errors ... ok
test_validate_financial_totals_incomplete_results ... ok
test_validate_financial_totals_matching_totals ... ok
test_validate_financial_totals_mismatching_totals ... ok
test_validate_financial_totals_within_tolerance ... ok
test_validate_table_row_counts_accurate_reporting ... ok
test_validate_table_row_counts_empty_tables ... ok
test_validate_table_row_counts_handles_errors ... ok

----------------------------------------------------------------------
Ran 20 tests in 0.049s

OK
```

## Notes

- Tests use mocking extensively, so no actual database connection is required
- Temporary files and directories are automatically cleaned up after tests complete
- The `logger` module attribute is mocked with `create=True` to handle the fact that the logger is created at runtime in the main script
