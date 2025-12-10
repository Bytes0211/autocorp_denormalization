-- ============================================================================
-- AutoCorp Denormalization: Index Creation
-- ============================================================================
-- Purpose: Create indexes on frequently queried columns to optimize
--          analytical query performance
-- Author: scotton
-- Date: December 10, 2025
-- ============================================================================

-- ============================================================================
-- Indexes for sales_order_fact
-- ============================================================================

-- Primary key index on order_id
CREATE INDEX IF NOT EXISTS idx_sales_order_fact_order_id 
ON sales_order_fact(order_id);

-- Customer dimension index
CREATE INDEX IF NOT EXISTS idx_sales_order_fact_customer_id 
ON sales_order_fact(customer_id);

-- Time-based queries index
CREATE INDEX IF NOT EXISTS idx_sales_order_fact_order_date 
ON sales_order_fact(order_date);

-- Invoice lookup index
CREATE INDEX IF NOT EXISTS idx_sales_order_fact_invoice_number 
ON sales_order_fact(invoice_number);

-- Status and type filters
CREATE INDEX IF NOT EXISTS idx_sales_order_fact_status 
ON sales_order_fact(status);

-- Geographic analysis
CREATE INDEX IF NOT EXISTS idx_sales_order_fact_state_city 
ON sales_order_fact(state, city);

-- ============================================================================
-- Indexes for sales_order_line_items
-- ============================================================================

-- Line item type discrimination
CREATE INDEX IF NOT EXISTS idx_sales_order_line_items_type 
ON sales_order_line_items(line_item_type);

-- Order relationship
CREATE INDEX IF NOT EXISTS idx_sales_order_line_items_order_id 
ON sales_order_line_items(order_id);

-- Customer dimension
CREATE INDEX IF NOT EXISTS idx_sales_order_line_items_customer_id 
ON sales_order_line_items(customer_id);

-- Time-based queries
CREATE INDEX IF NOT EXISTS idx_sales_order_line_items_order_date 
ON sales_order_line_items(order_date);

-- Item code lookup (SKU or ServiceID)
CREATE INDEX IF NOT EXISTS idx_sales_order_line_items_item_code 
ON sales_order_line_items(item_code);

-- Geographic analysis
CREATE INDEX IF NOT EXISTS idx_sales_order_line_items_state 
ON sales_order_line_items(state);

-- Composite index for revenue analysis by type and date
CREATE INDEX IF NOT EXISTS idx_sales_order_line_items_type_date 
ON sales_order_line_items(line_item_type, order_date);

-- ============================================================================
-- Indexes for service_parts_catalog
-- ============================================================================

-- Service lookup
CREATE INDEX IF NOT EXISTS idx_service_parts_catalog_serviceid 
ON service_parts_catalog(serviceid);

-- Part/SKU lookup
CREATE INDEX IF NOT EXISTS idx_service_parts_catalog_sku 
ON service_parts_catalog(sku);

-- Category analysis
CREATE INDEX IF NOT EXISTS idx_service_parts_catalog_category 
ON service_parts_catalog(category);

-- Vendor analysis
CREATE INDEX IF NOT EXISTS idx_service_parts_catalog_vendor 
ON service_parts_catalog(vendor);

-- Display index creation summary
SELECT 
    schemaname,
    tablename,
    indexname,
    indexdef
FROM pg_indexes
WHERE tablename IN ('sales_order_fact', 'sales_order_line_items', 'service_parts_catalog')
ORDER BY tablename, indexname;
