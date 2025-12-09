# Developer's Approach Document

**Project Name:** AutoCorp Data Denormalization Pipeline  
**Author:** scotton  
**Date:** December 8, 2025  
**Status:** Draft

---

## 1. Overview

### Problem Statement
The AutoCorp database is currently normalized across 7 tables with complex relationships, requiring multiple joins for analytical queries. This structure, while optimal for transactional consistency, creates performance bottlenecks for reporting and analytics. I need to denormalize this data into analytical-friendly wide tables that enable fast querying and eliminate the need for complex joins.

### Goals
- Create denormalized fact and dimension tables optimized for analytical queries
- Reduce query complexity by pre-joining related tables
- Improve query performance for reporting and analytics workloads
- Maintain data consistency through automated ETL pipelines
- Enable efficient data export for data lake integration

### Success Criteria
- Denormalized tables created with 100% data completeness
- Query performance improvement of >80% compared to normalized schema joins
- Automated pipeline that can refresh denormalized tables on-demand
- Data validation checks confirm accuracy (row counts and totals match source)
- Documented transformation logic for all denormalized structures

---

## 2. Background & Context

### Current State
The AutoCorp PostgreSQL database contains:
- **customers** (1,149 rows): Customer master data with contact information
- **sales_order** (397,146 rows): Sales order header with invoice, dates, totals
- **sales_order_parts** (junction): Links orders to auto parts
- **sales_order_services** (junction): Links orders to services
- **auto_parts** (400 rows): Parts catalog with SKU, pricing, vendor info
- **service** (110 rows): Service catalog with labor costs and time
- **service_parts** (junction): Links services to required parts

The normalized structure requires 3-5 table joins for common analytical queries like:
- Customer order history with line items
- Revenue analysis by customer and product
- Service-to-parts relationships for inventory planning

### Requirements
- Create a **sales_fact** table that combines orders with customer and line item details
- Create a **service_catalog** table with pre-joined parts requirements
- Maintain original data types and precision (especially for monetary values)
- Include all relevant dimensions for slice-and-dice analysis
- Support incremental refresh (identify changed records)
- Export denormalized data to CSV for data lake ingestion

### Constraints
- Source database is PostgreSQL with ~397K sales orders
- Must preserve referential integrity during denormalization
- Must handle one-to-many relationships (orders → line items)
- Development environment only (no production impact)
- Must use Python/SQL for transformation logic
- Output format: PostgreSQL tables + CSV exports

---

## 3. Design

1. Multi-Granularity Design
Creating both order-level (sales_order_fact) and line-item-level (sales_order_line_items) tables seems to be the best design. Different analysts need different granularity—executives want order summaries, while operations teams need line-item details. This avoids forcing one table to serve all purposes.

2. Strategic Denormalization
Embedding customer data directly into fact tables eliminates the most common join (orders → customers). Since customer data changes infrequently but is queried constantly, this trade-off makes sense.

3. Unified Line Items (UNION ALL)
Combining parts and services into a single table with a discriminator column (line_item_type) is excellent for cross-product revenue analysis. The NULL columns are a minor cost for major analytical flexibility.

4. Service Parts Catalog
Pre-joining services with required parts is practical for inventory planning and cost analysis. This reference table avoids repeated complex joins.

5. Pragmatic Technology Choices
•  Physical tables (not just views) enable CSV export and better query performance
•  Python + psycopg2 is straightforward and maintainable
•  Staying in PostgreSQL before moving to data lake is a logical progression

6. Data Lake Compatibility
The CSV export strategy aligns with the broader AutoCorp data lake project (referenced in the doc). This creates a bridge between OLTP and cloud analytics.

### Potential Improvements

1. Consider Incremental Refresh
The document mentions it but doesn't detail implementation. With 397K orders growing to 500K/year, full refreshes will eventually become expensive. Adding created_at/updated_at timestamps would enable CDC patterns.

2. Aggregation Tables
The open question about daily/monthly summaries is worth pursuing. Adding time-series rollups (orders by day, revenue by month) would further accelerate dashboards.

3. Partitioning Strategy
While mentioned for >10M orders, partitioning by order_date could be beneficial now for query pruning on date-range queries.

### Bottom Line

I consider this as a well-thought-out, production-ready approach. It balances simplicity, performance, and scalability while acknowledging trade-offs. The alternative analysis shows the author considered other options and made informed decisions. The risk mitigation (read replicas, compression, indexing) demonstrates operational awareness.

Rating: 8.5/10 — Strong design that will deliver measurable query performance improvements while positioning AutoCorp for data lake integration.

---

## 4. Proposed Solution

### High-Level Approach
I will create two primary denormalized structures:

1. **sales_order_fact**: A wide fact table that denormalizes sales orders with customer info and aggregated line item metrics
2. **sales_order_line_items**: A detailed fact table with individual line items (parts and services) joined with order and customer context
3. **service_parts_catalog**: A reference table denormalizing services with their required parts

The denormalization will be implemented using:
- SQL views for reusable query logic
- Python ETL scripts for data transformation and CSV export
- PostgreSQL materialized views for performance (optional)

### Key Components

1. **Denormalized Sales Fact Table**
   - Purpose: Single table for order-level analytics
   - Responsibilities: Contains order header, customer details, aggregated totals, and line item counts

2. **Denormalized Line Items Table**
   - Purpose: Detailed line-item analysis with full context
   - Responsibilities: Each row represents one line item (part or service) with order and customer details

3. **Service Parts Catalog**
   - Purpose: Pre-joined service-to-parts reference
   - Responsibilities: Shows which parts are required for each service

4. **ETL Pipeline**
   - Purpose: Orchestrates denormalization and export
   - Responsibilities: Execute SQL transformations, validate data, export to CSV

### Data Models

#### sales_order_fact
```sql
CREATE TABLE sales_order_fact AS
SELECT 
    so.order_id,
    so.invoice_number,
    so.order_date,
    so.order_type,
    so.status,
    so.payment_method,
    -- Customer dimensions
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
    -- Line item metrics
    COUNT(DISTINCT sop.line_item_id) as parts_line_count,
    COUNT(DISTINCT sos.line_item_id) as services_line_count,
    COALESCE(SUM(sop.quantity), 0) as total_parts_quantity,
    COALESCE(SUM(sos.quantity), 0) as total_services_quantity
FROM sales_order so
JOIN customers c ON so.customer_id = c.customer_id
LEFT JOIN sales_order_parts sop ON so.order_id = sop.order_id
LEFT JOIN sales_order_services sos ON so.order_id = sos.order_id
GROUP BY so.order_id, c.customer_id;
```

#### sales_order_line_items
```sql
CREATE TABLE sales_order_line_items AS
-- Parts line items
SELECT 
    'PART' as line_item_type,
    sop.line_item_id,
    so.order_id,
    so.invoice_number,
    so.order_date,
    so.status as order_status,
    c.customer_id,
    c.first_name || ' ' || c.last_name as customer_name,
    c.email as customer_email,
    c.city,
    c.state,
    sop.sku as item_code,
    ap.name as item_name,
    ap.description as item_description,
    sop.quantity,
    sop.unit_price,
    sop.line_total,
    sop.discount_percent,
    sop.discount_amount,
    NULL as labor_minutes,
    ap.vendor
FROM sales_order_parts sop
JOIN sales_order so ON sop.order_id = so.order_id
JOIN customers c ON so.customer_id = c.customer_id
JOIN auto_parts ap ON sop.sku = ap.sku

UNION ALL

-- Service line items
SELECT 
    'SERVICE' as line_item_type,
    sos.line_item_id,
    so.order_id,
    so.invoice_number,
    so.order_date,
    so.status as order_status,
    c.customer_id,
    c.first_name || ' ' || c.last_name as customer_name,
    c.email as customer_email,
    c.city,
    c.state,
    sos.serviceid as item_code,
    s.service as item_name,
    s.category as item_description,
    sos.quantity,
    sos.unit_price,
    sos.line_total,
    sos.discount_percent,
    sos.discount_amount,
    s.labor_minutes,
    NULL as vendor
FROM sales_order_services sos
JOIN sales_order so ON sos.order_id = so.order_id
JOIN customers c ON so.customer_id = c.customer_id
JOIN service s ON sos.serviceid = s.serviceid;
```

#### service_parts_catalog
```sql
CREATE TABLE service_parts_catalog AS
SELECT 
    s.serviceid,
    s.service,
    s.category,
    s.labor_minutes,
    s.labor_cost,
    sp.sku,
    ap.name as part_name,
    ap.description as part_description,
    sp.quantity as parts_quantity_required,
    ap.price as part_unit_price,
    (sp.quantity * ap.price) as total_parts_cost,
    ap.vendor
FROM service s
JOIN service_parts sp ON s.serviceid = sp.serviceid
JOIN auto_parts ap ON sp.sku = ap.sku;
```

### Design Decisions

- **Decision:** Use separate fact tables for order-level and line-level analysis
  - **Rationale:** Different granularity serves different analytical use cases (summary vs. detail)
  - **Trade-offs:** More storage, but better query performance for each use case

- **Decision:** Denormalize customer data into each fact table
  - **Rationale:** Eliminates customer dimension join in 90% of analytical queries
  - **Trade-offs:** Data duplication, but customers change infrequently

- **Decision:** UNION ALL parts and services into single line items table
  - **Rationale:** Unified view for revenue analysis across product types
  - **Trade-offs:** Some columns are NULL for certain line types, but enables cross-product analytics

- **Decision:** Export to CSV for data lake compatibility
  - **Rationale:** Aligns with existing AutoCorp data lake project (Phase 3 DMS/DataSync)
  - **Trade-offs:** Additional I/O, but enables hybrid analytics (PostgreSQL + S3)

---

## 4. Implementation Details

### Technology Stack
- **PostgreSQL 14+**: Source and target database for denormalized tables
- **Python 3.9+**: ETL orchestration, data validation, CSV export
- **psycopg2**: Python PostgreSQL adapter
- **pandas**: Data validation and CSV generation

### Key Implementation Areas

#### Area 1: SQL Denormalization Queries
Create SQL scripts that:
1. Drop existing denormalized tables if they exist
2. Create denormalized tables using complex SELECT statements with joins
3. Create indexes on frequently queried columns (order_id, customer_id, order_date)
4. Generate summary statistics for validation

#### Area 2: Python ETL Pipeline
Build a Python script that:
1. Connects to PostgreSQL
2. Executes denormalization SQL scripts
3. Validates row counts and totals
4. Exports denormalized tables to CSV
5. Logs execution metrics (duration, row counts)

#### Area 3: Data Validation
Implement checks to ensure:
- Row count consistency (denormalized ≈ source)
- Financial totals match (SUM aggregations)
- No NULL values in critical columns
- Referential integrity preserved

### Migration Strategy
This is a new analytical layer, not a replacement:
1. Create denormalized tables in same database with distinct names
2. Original normalized tables remain unchanged
3. Phase 1: Build and validate denormalized tables
4. Phase 2: Export to CSV for data lake integration
5. Phase 3: Schedule periodic refresh (daily/weekly)

---

## 5. Alternatives Considered

### Alternative 1: Materialized Views Only
- **Description:** Use PostgreSQL materialized views instead of physical tables
- **Pros:** Built-in refresh mechanism, less storage
- **Cons:** Slower refresh with large datasets, limited CSV export options
- **Why not chosen:** Need CSV exports for data lake; materialized views complicate export logic

### Alternative 2: Real-time Denormalization with Triggers
- **Description:** Use database triggers to maintain denormalized tables in real-time
- **Pros:** Always up-to-date, no batch refresh needed
- **Cons:** Complex trigger logic, performance impact on OLTP operations
- **Why not chosen:** AutoCorp is analytical use case, doesn't require real-time updates

### Alternative 3: Denormalize in Data Lake Only
- **Description:** Skip PostgreSQL denormalization, do it in AWS Glue/Athena
- **Pros:** Leverages cloud-native tools, separates OLTP and OLAP
- **Cons:** Requires data lake infrastructure to be operational first
- **Why not chosen:** Want standalone PostgreSQL solution that also exports to data lake

---

## 6. Testing Strategy

### Test Approach
1. **Unit Tests:** Validate individual SQL queries return expected structure
2. **Integration Tests:** Verify end-to-end ETL pipeline execution
3. **Data Quality Tests:** Compare source vs. denormalized aggregations
4. **Performance Tests:** Measure query performance improvement

### Key Test Scenarios
- Denormalize full dataset (397K orders) and validate totals
- Query denormalized tables without joins and measure performance
- Export CSV files and verify file sizes and row counts
- Re-run denormalization (idempotent operation) and verify results unchanged

### Edge Cases
- Orders with no line items (parts or services)
- Orders with only parts (no services) or vice versa
- Services with no required parts (service_parts_catalog)
- NULL values in optional customer fields (phone, address)

---

## 7. Non-Functional Considerations

### Performance
- **Expected ETL duration:** <5 minutes for full refresh (397K orders)
- **Query performance improvement:** 80%+ reduction in query time vs. joins
- **Indexes:** Create on order_id, customer_id, order_date, invoice_number
- **CSV export:** Use psycopg2 COPY command for fast export (~1-2 minutes)

### Security
- Use environment variables for database credentials
- No sensitive data in logs (mask customer PII)
- CSV files stored with restricted permissions (chmod 600)

### Scalability
- Current dataset: 397K orders, ~1.6M total rows across tables
- Projected growth: 500K orders/year
- Denormalization scales linearly with ORDER BY and LIMIT patterns
- Consider partitioning by order_date if >10M orders

### Observability
- Log file with timestamps for each ETL step
- Row count validation logged for each table
- CSV export file sizes logged
- Error handling with descriptive messages

---

## 8. Risks & Mitigations

| Risk | Impact | Probability | Mitigation |
|------|--------|-------------|------------|
| Denormalized tables too large for memory | HIGH | LOW | Use indexed queries, avoid SELECT * |
| Data inconsistency (source changes during ETL) | MEDIUM | LOW | Run during low-activity hours, add timestamps |
| CSV export file size too large | MEDIUM | MEDIUM | Use gzip compression, split by date ranges |
| Performance degradation on source DB | HIGH | LOW | Run denormalization on read replica |

---

## 9. Timeline & Milestones

### Phase 1: SQL Development (1 day)
- Duration: 1 day
- Deliverables: SQL scripts for all denormalized tables tested and validated

### Phase 2: Python ETL Pipeline (1 day)
- Duration: 1 day
- Deliverables: Python script that orchestrates denormalization and CSV export

### Phase 3: Testing & Documentation (0.5 days)
- Duration: 0.5 days
- Deliverables: Data validation tests, performance benchmarks, README

---

## 10. Open Questions

- [ ] Should denormalized tables be refreshed daily, weekly, or on-demand?
- [ ] Do I need incremental refresh (CDC) or full refresh each time?
- [ ] What compression format for CSV exports (gzip, bz2, none)?
- [ ] Should I create additional aggregate tables (daily/monthly summaries)?

---

## 11. References

- AutoCorp Data Lake project: `/home/scotton/dev/projects/autocorp/`
- PostgreSQL database: `autocorp` database on localhost
- Data lake project status: `/home/scotton/dev/projects/autocorp/project-status.md`
- Template source: `/home/scotton/dev/templates/developer-approach-template.md`
