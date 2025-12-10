-- ============================================================================
-- AutoCorp Denormalization: Sales Order Line Items Table
-- ============================================================================
-- Purpose: Create detailed line-item fact table combining parts and services
--          with full order and customer context for granular analytics
-- Expected Rows: ~1.2M (853,591 parts + 355,067 services line items)
-- Author: scotton
-- Date: December 10, 2025
-- ============================================================================

-- Drop table if exists (for idempotent execution)
DROP TABLE IF EXISTS sales_order_line_items CASCADE;

-- Create denormalized line items table (UNION ALL parts and services)
CREATE TABLE sales_order_line_items AS

-- Parts line items
SELECT 
    'PART'::VARCHAR(10) as line_item_type,
    sop.line_item_id,
    so.order_id,
    so.invoice_number,
    so.order_date,
    so.status as order_status,
    
    -- Customer dimensions
    c.customer_id,
    (c.first_name || ' ' || c.last_name) as customer_name,
    c.email as customer_email,
    c.city,
    c.state,
    
    -- Line item details
    sop.sku as item_code,
    ap.name as item_name,
    ap.description as item_description,
    sop.quantity,
    sop.unit_price,
    sop.line_total,
    
    -- Parts-specific fields
    ap.vendor,
    NULL::INTEGER as labor_minutes,
    NULL::NUMERIC(10,2) as labor_cost,
    NULL::NUMERIC(10,2) as parts_cost
    
FROM sales_order_parts sop
JOIN sales_order so ON sop.order_id = so.order_id
JOIN customers c ON so.customer_id = c.customer_id
JOIN auto_parts ap ON sop.sku = ap.sku

UNION ALL

-- Service line items
SELECT 
    'SERVICE'::VARCHAR(10) as line_item_type,
    sos.line_item_id,
    so.order_id,
    so.invoice_number,
    so.order_date,
    so.status as order_status,
    
    -- Customer dimensions
    c.customer_id,
    (c.first_name || ' ' || c.last_name) as customer_name,
    c.email as customer_email,
    c.city,
    c.state,
    
    -- Line item details
    sos.serviceid as item_code,
    s.service as item_name,
    s.category as item_description,
    sos.quantity,
    (sos.line_total / NULLIF(sos.quantity, 0))::NUMERIC(10,2) as unit_price,
    sos.line_total,
    
    -- Service-specific fields
    NULL::VARCHAR(100) as vendor,
    sos.labor_minutes,
    sos.labor_cost,
    sos.parts_cost
    
FROM sales_order_services sos
JOIN sales_order so ON sos.order_id = so.order_id
JOIN customers c ON so.customer_id = c.customer_id
JOIN service s ON sos.serviceid = s.serviceid;

-- Display row count breakdown
SELECT 
    line_item_type,
    COUNT(*) as row_count,
    SUM(line_total) as total_revenue
FROM sales_order_line_items
GROUP BY line_item_type
ORDER BY line_item_type;

-- Display total row count
SELECT COUNT(*) as total_line_items FROM sales_order_line_items;

-- Display sample records
SELECT * FROM sales_order_line_items LIMIT 5;
