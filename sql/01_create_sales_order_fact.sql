-- ============================================================================
-- AutoCorp Denormalization: Sales Order Fact Table
-- ============================================================================
-- Purpose: Create denormalized sales order fact table with customer info
--          and aggregated line item metrics for order-level analytics
-- Expected Rows: 397,146 (one per order)
-- Author: scotton
-- Date: December 10, 2025
-- ============================================================================

-- Drop table if exists (for idempotent execution)
DROP TABLE IF EXISTS sales_order_fact CASCADE;

-- Create denormalized sales order fact table
CREATE TABLE sales_order_fact AS
SELECT 
    -- Order identifiers and header info
    so.order_id,
    so.invoice_number,
    so.order_date,
    so.order_type,
    so.status,
    so.payment_method,
    
    -- Customer dimensions (denormalized to avoid joins)
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.phone,
    c.city,
    c.state,
    c.zip_code,
    
    -- Order financials
    so.subtotal,
    so.tax,
    so.total_amount,
    
    -- Aggregated line item metrics
    COUNT(DISTINCT sop.line_item_id) as parts_line_count,
    COUNT(DISTINCT sos.line_item_id) as services_line_count,
    COALESCE(SUM(sop.quantity), 0) as total_parts_quantity,
    COALESCE(SUM(sos.quantity), 0) as total_services_quantity,
    
    -- Total line items
    (COUNT(DISTINCT sop.line_item_id) + COUNT(DISTINCT sos.line_item_id)) as total_line_items
    
FROM sales_order so
JOIN customers c ON so.customer_id = c.customer_id
LEFT JOIN sales_order_parts sop ON so.order_id = sop.order_id
LEFT JOIN sales_order_services sos ON so.order_id = sos.order_id
GROUP BY 
    so.order_id, 
    so.invoice_number,
    so.order_date,
    so.order_type,
    so.status,
    so.payment_method,
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.phone,
    c.city,
    c.state,
    c.zip_code,
    so.subtotal,
    so.tax,
    so.total_amount;

-- Display row count
SELECT COUNT(*) as sales_order_fact_row_count FROM sales_order_fact;

-- Display sample records
SELECT * FROM sales_order_fact LIMIT 5;
