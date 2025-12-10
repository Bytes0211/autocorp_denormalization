-- ============================================================================
-- AutoCorp Denormalization: Service Parts Catalog
-- ============================================================================
-- Purpose: Create reference table showing which parts are required for each
--          service, pre-joined for inventory planning and cost analysis
-- Expected Rows: Variable (depends on service_parts junction table)
-- Author: scotton
-- Date: December 10, 2025
-- ============================================================================

-- Drop table if exists (for idempotent execution)
DROP TABLE IF EXISTS service_parts_catalog CASCADE;

-- Create service parts catalog
CREATE TABLE service_parts_catalog AS
SELECT 
    -- Service information
    s.serviceid,
    s.service,
    s.category,
    s.labor_minutes,
    s.labor_cost,
    
    -- Parts information
    sp.sku,
    ap.name as part_name,
    ap.description as part_description,
    sp.quantity as parts_quantity_required,
    ap.price as part_unit_price,
    
    -- Calculated costs
    (sp.quantity * ap.price) as total_parts_cost,
    (s.labor_cost::NUMERIC + (sp.quantity * ap.price)) as total_service_cost,
    
    -- Vendor information
    ap.vendor
    
FROM service s
JOIN service_parts sp ON s.serviceid = sp.serviceid
JOIN auto_parts ap ON sp.sku = ap.sku
ORDER BY s.serviceid, ap.sku;

-- Display row count
SELECT COUNT(*) as service_parts_catalog_row_count FROM service_parts_catalog;

-- Display cost summary by service
SELECT 
    serviceid,
    service,
    COUNT(*) as parts_required,
    SUM(parts_quantity_required) as total_parts_quantity,
    SUM(total_parts_cost) as total_parts_cost,
    AVG(labor_cost::NUMERIC) as labor_cost
FROM service_parts_catalog
GROUP BY serviceid, service
ORDER BY total_parts_cost DESC
LIMIT 10;

-- Display sample records
SELECT * FROM service_parts_catalog LIMIT 5;
