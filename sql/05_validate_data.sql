-- ============================================================================
-- AutoCorp Denormalization: Data Validation
-- ============================================================================
-- Purpose: Validate denormalized tables against source data to ensure
--          data quality, completeness, and accuracy
-- Author: scotton
-- Date: December 10, 2025
-- ============================================================================

-- ============================================================================
-- Validation 1: Row Count Checks
-- ============================================================================

\echo '========================================='
\echo 'VALIDATION 1: Row Count Checks'
\echo '========================================='

-- Source table row counts
SELECT 'Source: sales_order' as table_name, COUNT(*) as row_count FROM sales_order
UNION ALL
SELECT 'Source: sales_order_parts', COUNT(*) FROM sales_order_parts
UNION ALL
SELECT 'Source: sales_order_services', COUNT(*) FROM sales_order_services
UNION ALL
SELECT 'Source: customers', COUNT(*) FROM customers
UNION ALL
SELECT 'Denormalized: sales_order_fact', COUNT(*) FROM sales_order_fact
UNION ALL
SELECT 'Denormalized: sales_order_line_items', COUNT(*) FROM sales_order_line_items
UNION ALL
SELECT 'Denormalized: service_parts_catalog', COUNT(*) FROM service_parts_catalog;

-- ============================================================================
-- Validation 2: Financial Totals Verification
-- ============================================================================

\echo '========================================='
\echo 'VALIDATION 2: Financial Totals'
\echo '========================================='

-- Compare total_amount from source vs denormalized
SELECT 
    'Source (sales_order)' as source,
    SUM(total_amount) as total_revenue,
    SUM(subtotal) as total_subtotal,
    SUM(tax) as total_tax
FROM sales_order
UNION ALL
SELECT 
    'Denormalized (sales_order_fact)',
    SUM(total_amount),
    SUM(subtotal),
    SUM(tax)
FROM sales_order_fact;

-- Compare line totals
SELECT 
    'Source: Parts' as source,
    SUM(line_total) as total_line_amount
FROM sales_order_parts
UNION ALL
SELECT 
    'Source: Services',
    SUM(line_total)
FROM sales_order_services
UNION ALL
SELECT 
    'Denormalized: Parts',
    SUM(line_total)
FROM sales_order_line_items
WHERE line_item_type = 'PART'
UNION ALL
SELECT 
    'Denormalized: Services',
    SUM(line_total)
FROM sales_order_line_items
WHERE line_item_type = 'SERVICE';

-- ============================================================================
-- Validation 3: Referential Integrity
-- ============================================================================

\echo '========================================='
\echo 'VALIDATION 3: Referential Integrity'
\echo '========================================='

-- Check for orphaned records (should return 0)
SELECT 
    'Orphaned orders in sales_order_fact' as check_name,
    COUNT(*) as issue_count
FROM sales_order_fact sof
WHERE NOT EXISTS (
    SELECT 1 FROM sales_order so 
    WHERE so.order_id = sof.order_id
);

-- Check for missing customers (should return 0)
SELECT 
    'Missing customers in sales_order_fact' as check_name,
    COUNT(*) as issue_count
FROM sales_order_fact sof
WHERE NOT EXISTS (
    SELECT 1 FROM customers c 
    WHERE c.customer_id = sof.customer_id
);

-- Check line items belong to valid orders (should return 0)
SELECT 
    'Orphaned line items' as check_name,
    COUNT(*) as issue_count
FROM sales_order_line_items soli
WHERE NOT EXISTS (
    SELECT 1 FROM sales_order so 
    WHERE so.order_id = soli.order_id
);

-- ============================================================================
-- Validation 4: Data Quality Checks
-- ============================================================================

\echo '========================================='
\echo 'VALIDATION 4: Data Quality'
\echo '========================================='

-- Check for NULL values in critical columns (sales_order_fact)
SELECT 
    'sales_order_fact: NULL order_id' as check_name,
    COUNT(*) as issue_count
FROM sales_order_fact
WHERE order_id IS NULL
UNION ALL
SELECT 
    'sales_order_fact: NULL customer_id',
    COUNT(*)
FROM sales_order_fact
WHERE customer_id IS NULL
UNION ALL
SELECT 
    'sales_order_fact: NULL total_amount',
    COUNT(*)
FROM sales_order_fact
WHERE total_amount IS NULL;

-- Check for NULL values in critical columns (sales_order_line_items)
SELECT 
    'sales_order_line_items: NULL order_id' as check_name,
    COUNT(*) as issue_count
FROM sales_order_line_items
WHERE order_id IS NULL
UNION ALL
SELECT 
    'sales_order_line_items: NULL line_total',
    COUNT(*)
FROM sales_order_line_items
WHERE line_total IS NULL;

-- Check for negative quantities or prices (should return 0)
SELECT 
    'Negative quantities in line items' as check_name,
    COUNT(*) as issue_count
FROM sales_order_line_items
WHERE quantity < 0
UNION ALL
SELECT 
    'Negative prices in line items',
    COUNT(*)
FROM sales_order_line_items
WHERE unit_price < 0;

-- ============================================================================
-- Validation 5: Aggregation Accuracy
-- ============================================================================

\echo '========================================='
\echo 'VALIDATION 5: Aggregation Accuracy'
\echo '========================================='

-- Verify line item counts match between source and denormalized
WITH source_counts AS (
    SELECT 
        so.order_id,
        COUNT(DISTINCT sop.line_item_id) as parts_count,
        COUNT(DISTINCT sos.line_item_id) as services_count
    FROM sales_order so
    LEFT JOIN sales_order_parts sop ON so.order_id = sop.order_id
    LEFT JOIN sales_order_services sos ON so.order_id = sos.order_id
    GROUP BY so.order_id
),
denorm_counts AS (
    SELECT 
        order_id,
        parts_line_count,
        services_line_count
    FROM sales_order_fact
)
SELECT 
    'Orders with mismatched parts count' as check_name,
    COUNT(*) as issue_count
FROM source_counts sc
JOIN denorm_counts dc ON sc.order_id = dc.order_id
WHERE sc.parts_count != dc.parts_line_count
UNION ALL
SELECT 
    'Orders with mismatched services count',
    COUNT(*)
FROM source_counts sc
JOIN denorm_counts dc ON sc.order_id = dc.order_id
WHERE sc.services_count != dc.services_line_count;

-- ============================================================================
-- Validation Summary
-- ============================================================================

\echo '========================================='
\echo 'VALIDATION COMPLETE'
\echo '========================================='
\echo 'All checks with issue_count = 0 indicate successful validation'
\echo 'Any non-zero counts should be investigated'
