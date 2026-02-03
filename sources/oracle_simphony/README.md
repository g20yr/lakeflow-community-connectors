# Oracle Simphony BI API Connector

This connector provides access to Oracle Simphony Business Intelligence (BI) API for extracting point-of-sale (POS), payment, labor, and operational data into Databricks.

## Overview

Oracle Simphony is a comprehensive POS and hospitality management platform. This connector enables data ingestion from the Simphony BI API, which provides:
- Daily aggregations of sales, discounts, tender media, taxes, and service charges
- Transaction-level data (guest checks, payments, non-sales transactions)
- Kitchen display system performance metrics
- Fiscal transaction data
- Dimension data (locations)
- Cash management details

**API Version**: 20.1
**Documentation**: https://docs.oracle.com/en/industries/food-beverage/back-office/20.1/biapi/

## Supported Tables (23 Working Endpoints)

All endpoint schemas have been validated against actual API responses and are fully functional!

### Daily Aggregations (12 tables) ✅ All working
- `combo_item_daily_totals` - Combo item sales by day
- `control_daily_totals` - Control totals by revenue center by day
- `discount_daily_totals` - Discount usage by day
- `employee_daily_totals` - Employee performance metrics by day
- `job_code_daily_totals` - Job code totals by day
- `menu_item_daily_totals` - Menu item sales by day
- `operations_daily_totals` - Revenue center operations by day
- `order_type_daily_totals` - Order type totals by day (Fixed: field naming)
- `revenue_center_daily_totals` - Revenue center totals by day
- `service_charge_daily_totals` - Service charge totals by day
- `tax_daily_totals` - Tax totals by day
- `tender_media_daily_totals` - Payment tender totals by day

### Cash Management (1 table)
- `cash_management_details` - Cash management transactions

### Fiscal Transactions (3 tables)
- `fiscal_invoice_control_data` - Fiscal invoice control data
- `fiscal_invoice_data` - Fiscal invoice transactions
- `fiscal_total_data` - Fiscal total transactions

### Kitchen Performance (1 table)
- `kds_details` - Kitchen display system performance metrics

### Transactions (6 tables) ✅ All working
- `guest_checks` - Guest check transaction headers (Fixed: nested structure)
- `guest_check_line_item_ext_details` - Guest check line item extensibility data (Fixed: nested structure)
- `non_sales_transactions` - Non-sales transactions (Fixed: nested structure)
- `pos_journal_log_details` - POS journal log data (Fixed: nested structure)
- `pos_waste_details` - POS waste transaction data (Fixed: nested structure)
- `spi_payment_details` - SPI payment transaction details

### Dimensions (1 table)
- `location_dimensions` - Location information and workstation definitions (Fixed: nested structure)
  - Includes `active_from` field: Start date of the location (derived from `openDt`)

## Configuration

### Multi-Location Mode

The connector operates in **multi-location mode** by default, automatically fetching data from all active locations. This eliminates the need to create separate connections for each location.

**Three Operating Modes:**

1. **Multi-Location (Recommended)** - Fetches all active locations automatically
2. **Single Location** - Filter to a specific location
3. **Multiple Locations** - Filter to specific locations

### Configuration Examples

#### Multi-Location Mode (Recommended)

Fetches data from all active locations automatically:

```json
{
  "bearer_token": "your_bearer_token_here",
  "org_identifier": "your_organization_identifier",
  "base_url": "https://your-instance.hospitality.oracleindustry.com/bi/v1"
}
```

#### Single Location Mode

Filter to a specific location (backwards compatible):

```json
{
  "bearer_token": "your_bearer_token_here",
  "org_identifier": "your_organization_identifier",
  "loc_ref": "29",
  "base_url": "https://your-instance.hospitality.oracleindustry.com/bi/v1"
}
```

#### Multiple Location Filter

Filter to specific locations (comma-separated):

```json
{
  "bearer_token": "your_bearer_token_here",
  "org_identifier": "your_organization_identifier",
  "loc_ref": "29,30,31",
  "base_url": "https://your-instance.hospitality.oracleindustry.com/bi/v1"
}
```

**Credential Details:**
- **bearer_token** (required): OAuth 2.0 Bearer token for API authentication
- **org_identifier** (required): Unique organization identifier (used in API URL path)
- **loc_ref** (optional): Location reference number(s) for filtering
  - If omitted: Fetches all active locations
  - Single location: `"29"`
  - Multiple locations: `"29,30,31"` (comma-separated)
- **base_url** (required): Your Oracle Simphony instance base URL (includes `/bi/v1` path)

### Performance Considerations

**Multi-Location Mode with Spark Parallelization**:
- **Parallelization**: Databricks/Spark automatically parallelizes across locations
- **API calls per table**: N (where N = number of locations)
- **Time per table**: ~1-3 seconds with parallel execution
- **Example**: 10 locations with 5 executors = ~2-4 seconds (vs 15-25 seconds sequential)
- **Scalability**: Linear scaling with executor count
  - 100 locations with 20 executors = ~5-10 seconds total
  - Each executor processes locations in parallel
- **Memory**: Constant per executor (one location at a time)
- **Checkpointing**: Spark checkpoints after each location for fault tolerance

### Optional Configuration

Additional optional parameters can be included:

```json
{
  "bearer_token": "...",
  "org_identifier": "...",
  "loc_ref": "...",
  "base_url": "...",
  "timeout": 60
}
```

- **timeout**: Request timeout in seconds (default: 30)

## Usage

### Basic Example

```python
from oracle_simphony import LakeflowConnect

# Initialize connector
connector = LakeflowConnect({
    "bearer_token": "your_token",
    "org_identifier": "DNB",
    "loc_ref": "29",
    "base_url": "https://your-instance.hospitality.oracleindustry.com/bi/v1"
})

# List available tables
tables = connector.list_tables()
print(f"Available tables: {tables}")

# Get schema for a table
schema = connector.get_table_schema("employee_daily_totals", {})
print(f"Schema: {schema}")

# Get metadata
metadata = connector.read_table_metadata("employee_daily_totals", {})
print(f"Metadata: {metadata}")

# Read data for a specific business date
records, offset = connector.read_table(
    "employee_daily_totals",
    start_offset={},
    table_options={"bus_dt": "2024-01-20"}
)

for record in records:
    print(record)
```

### Automatic Business Date Selection

**NEW**: The connector automatically fetches the latest available business date for each location using the `getLatestBusDt` API endpoint. This eliminates the need to manually specify business dates in most cases.

**How it works**:
1. When no business date is specified, the connector automatically calls the `getLatestBusDt` API for each location
2. Each location can have a different latest business date (cached per location)
3. The connector uses the latest finalized business date available in Oracle Simphony
4. This ensures you always get the most recent complete data without manual date management

**Automatic Mode (Recommended)**:
```python
# No business date specified - connector automatically fetches latest for each location
records, offset = connector.read_table(
    "employee_daily_totals",
    start_offset={},
    table_options={}
)
# Console output: [INFO] Latest business date for location 29: 2026-01-30
```

### Overriding Business Date

You can override the automatic business date selection in three ways:

**Priority 1 - Via start_offset** (for continuation/incremental reads):
```python
records, offset = connector.read_table(
    "employee_daily_totals",
    start_offset={"bus_dt": "2024-01-20"},
    table_options={}
)
```

**Priority 2 - Via table_options** (for explicit date override):
```python
# Specify a specific historical business date
records, offset = connector.read_table(
    "employee_daily_totals",
    start_offset={},
    table_options={"bus_dt": "2024-01-20"}
)
```

**Priority 3 - Automatic (default)**: If neither `start_offset` nor `table_options` contain a `bus_dt`, the connector automatically fetches the latest business date from the API.

**Priority 4 - Current date fallback**: Only used if the API call fails (rare).

### Business Date Resolution Priority

The connector determines which business date to use in this order:
1. **start_offset["bus_dt"]** - Continuation from previous read (highest priority)
2. **table_options["bus_dt"]** - Explicit user override
3. **API latest business date** - Automatically fetched per location (NEW!)
4. **Current date** - Fallback only if API fails (lowest priority)

**Note**: The `location_dimensions` table does NOT require `busDt` or `locRef` parameters.

### Reading Historical Data

**Option 1: Use Automatic Latest Business Date (Recommended)**

For most recent data, simply omit the business date and let the connector fetch the latest:

```python
# Fetches latest available business date automatically
records, offset = connector.read_table(
    "employee_daily_totals",
    start_offset={},
    table_options={}
)
# The connector handles business date selection automatically
```

**Option 2: Iterate Through Specific Date Range**

To read data for a specific historical date range, iterate through dates:

```python
from datetime import datetime, timedelta

start_date = datetime(2024, 1, 1)
end_date = datetime(2024, 1, 31)
current_date = start_date

while current_date <= end_date:
    bus_dt = current_date.strftime("%Y-%m-%d")

    records, offset = connector.read_table(
        "employee_daily_totals",
        start_offset={},
        table_options={"bus_dt": bus_dt}
    )

    for record in records:
        # Process record
        print(f"Date: {bus_dt}, Record: {record}")

    current_date += timedelta(days=1)
```

**Option 3: Let DLT Handle Date Ranges**

For production pipelines, configure Delta Live Tables to handle historical loads:

```python
import dlt
from pyspark.sql import functions as F

@dlt.table(
    name="employee_daily_totals_bronze",
    comment="Raw employee daily totals from Oracle Simphony"
)
def employee_daily_totals():
    # DLT automatically handles incremental processing
    # No need to specify business dates - connector fetches latest automatically
    return (
        spark.readStream
        .format("lakehouse")
        .option("connectionName", "my_oracle_conn")
        .option("tableName", "employee_daily_totals")
        .load()
    )
```

## Schema Structure

### Nested Structures

Most aggregation tables preserve the nested structure from the API response. For example, `employee_daily_totals`:

```json
{
  "locRef": "29",
  "busDt": "2024-01-20",
  "revenueCenters": [
    {
      "rvcNum": 10,
      "employees": [
        {
          "empNum": 884555,
          "netSlsTtl": 1234.56,
          "chkCnt": 25,
          "gstCnt": 45
        }
      ]
    }
  ]
}
```

This nested structure is preserved in the Spark schema using `ArrayType` and `StructType`.

### Flat Structures

Transaction and dimension tables use flat structures or have their own specific formats. Refer to the Oracle Simphony BI API documentation for detailed schemas.

## Testing

### Prerequisites for Testing

Before testing, ensure you have:
1. **Valid Oracle Simphony credentials**:
   - Bearer token (OAuth 2.0)
   - Organization identifier
   - Base URL for your instance
   - (Optional) Location reference for filtering

2. **Configuration file**: Create `sources/oracle_simphony/configs/dev_config.json`:
```json
{
  "bearer_token": "your_bearer_token_here",
  "org_identifier": "your_org_id",
  "base_url": "https://your-instance.hospitality.oracleindustry.com/bi/v1",
  "loc_ref": "29"
}
```

**Note**: For multi-location testing, omit the `loc_ref` parameter to fetch all active locations.

### Command-Line Testing

**Method 1: Standard Python Test Suite**
```bash
cd sources/oracle_simphony
python test/test_oracle_simphony_lakeflow_connect.py
```

**Method 2: Using UV (Recommended)**
```bash
# From repository root
uv run python sources/oracle_simphony/test/test_oracle_simphony_lakeflow_connect.py
```

**Method 3: Using Pytest**
```bash
# Run all Oracle Simphony tests
pytest sources/oracle_simphony/test/ -v

# Run specific test file
pytest sources/oracle_simphony/test/test_oracle_simphony_lakeflow_connect.py -v
```

**Method 4: Interactive Python Testing**
```bash
cd sources/oracle_simphony
python -c "
from oracle_simphony import LakeflowConnect
import json

# Load config
with open('configs/dev_config.json') as f:
    config = json.load(f)

# Initialize connector
conn = LakeflowConnect(config)

# Test basic functionality
print(f'Tables: {conn.list_tables()}')

# Test automatic business date fetching
records, offset = conn.read_table('employee_daily_totals', {}, {})
print(f'Fetched with automatic business date. Offset: {offset}')
"
```

### Testing via Databricks UI

**Step 1: Deploy Connector to Databricks**

Use the community connector CLI tool to deploy:

```bash
# Install the CLI tool
pip install -e tools/community_connector

# Create Unity Catalog connection
community-connector create_connection oracle_simphony my_oracle_conn \
  --options '{
    "bearer_token": "your_token",
    "org_identifier": "your_org_id",
    "base_url": "https://your-instance.hospitality.oracleindustry.com/bi/v1"
  }' \
  --spec sources/oracle_simphony/connector_spec.yaml
```

**Step 2: Test Connection in Databricks Notebook**

Create a new notebook in Databricks and run:

```python
# Test 1: List tables from the connection
df = spark.read.format("lakehouse") \
    .option("connectionName", "my_oracle_conn") \
    .load()

tables = df.select("table_name").distinct().collect()
print(f"Available tables: {[row.table_name for row in tables]}")

# Test 2: Read data with automatic business date
df = spark.read.format("lakehouse") \
    .option("connectionName", "my_oracle_conn") \
    .option("tableName", "employee_daily_totals") \
    .load()

df.show(5)
print(f"Row count: {df.count()}")

# Test 3: Read data with specific business date
df = spark.read.format("lakehouse") \
    .option("connectionName", "my_oracle_conn") \
    .option("tableName", "employee_daily_totals") \
    .option("bus_dt", "2024-01-20") \
    .load()

df.show(5)
```

**Step 3: Create Delta Live Tables (DLT) Pipeline**

```bash
# Create DLT pipeline using CLI
community-connector create_pipeline oracle_simphony my_oracle_pipeline \
  --connection-name my_oracle_conn \
  --catalog main \
  --target oracle_simphony_raw

# Or manually in Databricks UI:
# 1. Go to Delta Live Tables
# 2. Create new pipeline
# 3. Add notebook with @dlt.table decorators
# 4. Configure connection name in pipeline settings
```

**Step 4: Test in SQL Warehouse**

After DLT pipeline creates tables:

```sql
-- List all Oracle Simphony tables
SHOW TABLES IN main.oracle_simphony_raw;

-- Query employee daily totals
SELECT * FROM main.oracle_simphony_raw.employee_daily_totals LIMIT 10;

-- Check location dimensions with active_from field
SELECT locRef, name, active, active_from
FROM main.oracle_simphony_raw.location_dimensions
ORDER BY active_from DESC;

-- Aggregate sales by location
SELECT
  locRef,
  COUNT(*) as record_count,
  busDt
FROM main.oracle_simphony_raw.employee_daily_totals
GROUP BY locRef, busDt
ORDER BY busDt DESC;
```

### Testing Multi-Location Mode

**Test automatic location discovery**:
```python
# Remove loc_ref from config to test multi-location mode
config = {
    "bearer_token": "your_token",
    "org_identifier": "your_org_id",
    "base_url": "https://your-instance.hospitality.oracleindustry.com/bi/v1"
    # loc_ref omitted - will fetch all active locations
}

conn = LakeflowConnect(config)

# Should see output like:
# [INFO] Found 10 active locations
#   - Location 29: Main Restaurant
#   - Location 30: Bar Area
#   ...

records, offset = conn.read_table('employee_daily_totals', {}, {})
# Should see output like:
# [INFO] Reading employee_daily_totals for 10 location(s)...
# [INFO] Fetching latest business date for location 29...
# [INFO] Latest business date for location 29: 2026-01-30
# [INFO] Fetched 125 records from location 29
# ...
```

### Testing Automatic Business Date

**Test 1: Automatic Mode (No date specified)**
```python
# Connector automatically fetches latest business date
records, offset = conn.read_table('employee_daily_totals', {}, {})
print(f"Offset: {offset}")
# Output: {"bus_dt": "2026-01-30", "current_location_index": 1, "completed": False}
```

**Test 2: Manual Override**
```python
# Explicitly specify historical date
records, offset = conn.read_table(
    'employee_daily_totals',
    {},
    {'bus_dt': '2024-01-20'}
)
print(f"Offset: {offset}")
# Output: {"bus_dt": "2024-01-20", "current_location_index": 1, "completed": False}
```

**Test 3: Verify Caching**
```python
# First call fetches and caches
conn.read_table('employee_daily_totals', {}, {})
# Console: [INFO] Fetching latest business date for location 29...
# Console: [INFO] Latest business date for location 29: 2026-01-30

# Second call uses cache (no API call)
conn.read_table('menu_item_daily_totals', {}, {})
# Console: (no fetching message - uses cached date)
```

### Test Coverage

The test suite validates:
- ✓ Connector initialization with credentials
- ✓ Multi-location discovery and filtering
- ✓ Automatic latest business date fetching per location
- ✓ Business date caching and reuse
- ✓ List tables returns all available endpoints
- ✓ Schema generation for all tables (LongType validation)
- ✓ Metadata configuration (primary keys, ingestion type)
- ✓ Data reading from API with automatic and manual business dates
- ✓ Error handling (400, 401, 403, 404)
- ✓ Offset tracking for incremental reads

## Endpoint Parameter Requirements

Different endpoints have different parameter requirements:

| Category | locRef | busDt | applicationName |
|----------|--------|-------|-----------------|
| Daily Aggregations | ✅ Required | ✅ Required | ⚪ Optional |
| Fiscal Transactions | ✅ Required | ✅ Required | ⚪ Optional |
| Cash Management | ✅ Required | ✅ Required | ⚪ Optional |
| Kitchen Performance | ✅ Required | ✅ Required | ⚪ Optional |
| Most Transactions | ✅ Required | ✅ Required | ⚪ Optional |
| SPI Payment Details | ✅ Required | ✅ Required | ❌ NOT Supported |
| Location Dimensions | ❌ NOT Required | ❌ NOT Required | ⚪ Optional |

## Ingestion Type

All tables use **snapshot** ingestion:
- Data is queried by specific business date (`busDt`)
- Each read returns complete data for that date
- No incremental cursor or delete tracking
- Pipeline orchestration handles date-range queries

## Data Types

The connector follows Lakeflow conventions for data types:
- **Integers** → `LongType` (never `IntegerType`)
- **Decimals** → `DoubleType`
- **Strings/Dates** → `StringType`
- **Nested Objects** → `StructType`
- **Arrays** → `ArrayType`

## Error Handling

The connector handles common API errors:

| Status Code | Meaning | Action |
|------------|---------|--------|
| 200 | Success | Data returned |
| 400 | Bad Request | Check request parameters |
| 401 | Unauthorized | Verify bearer token |
| 403 | Forbidden | Check API permissions |
| 404 | Not Found | Check endpoint or date |
| 429 | Rate Limited | Implement retry logic |
| 500/503 | Server Error | Retry later |

## Limitations

1. **Date-Based Queries**: Most queries are based on business date (finalized data, not real-time)
   - ✅ **MITIGATED**: Automatic latest business date fetching eliminates manual date management
2. **No Pagination**: API returns complete result sets (no pagination documented)
3. **No Real-Time Data**: Data is available based on business date processing
   - Latest business date may be 1-2 days behind current date depending on data finalization
4. **Instance Specific**: Endpoint availability varies by Oracle Simphony instance configuration
   - Use test suite to discover available endpoints for your instance
5. **Bearer Token Expiration**: Tokens expire and need to be refreshed periodically

## Endpoint Availability

**Note**: Not all documented Oracle Simphony BI API endpoints may be available in your instance. The 23 endpoints listed in this README have been verified to work. Your instance may have:
- Different API versions
- Different licensing/feature enablement
- Permission restrictions on certain endpoints

Run `test/test_all_documented_endpoints.py` to discover which endpoints are available in your specific instance.

## Troubleshooting

### Authentication Errors (401)
- Verify `bearer_token` is valid and not expired
- Check token has appropriate permissions for the API

### Not Found Errors (404)
- Verify `org_identifier` is correct
- Check that data exists for the specified `busDt`
- Ensure endpoint is available in your instance

### Empty Results
- With automatic business date: The connector uses the latest available date, so empty results likely mean no data exists
- With manual business date: Verify data exists for the specified business date
- Check that location has activity on that date
- Some tables may be empty if features are not used
- Check console logs for the business date being used: `[INFO] Latest business date for location 29: 2026-01-30`

### Connection Timeout
- Increase `timeout` parameter in configuration
- Check network connectivity to Oracle Simphony API
- Verify API endpoint URL is accessible

### Wrong Base URL
- Ensure `base_url` includes `/bi/v1` path component
- Format: `https://your-instance.hospitality.oracleindustry.com/bi/v1`
- Do NOT use: `https://api.oracle.com/simphony/bi/v1` (generic URL)

## API Reference

- **Base URL Pattern**: `https://{instance}.hospitality.oracleindustry.com/bi/v1/{orgIdentifier}`
- **Method**: POST for all endpoints
- **Authentication**: Bearer token in Authorization header
- **Content-Type**: application/json

**Example Request**:
```bash
curl -X POST \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "locRef": "29",
    "busDt": "2024-01-20"
  }' \
  https://your-instance.hospitality.oracleindustry.com/bi/v1/DNB/getEmployeeDailyTotals
```

## Not Yet Implemented

The following endpoint categories from the Oracle Simphony BI API documentation are not yet available in this connector or your instance:

- **Quarter-Hour Aggregations** (8 endpoints) - Not available in tested instance
- **Labor** (1 endpoint) - Time card details not available
- **Most POS Dimensions** (15 endpoints) - Not available except Location Dimensions
- **Payment Dimensions** (2 endpoints) - Not available
- **Payment Transactions** (5 endpoints) - Permission restricted

Check the [official Oracle documentation](https://docs.oracle.com/en/industries/food-beverage/back-office/20.1/biapi/) for the complete list of 67 documented endpoints.

## Support

For issues or questions:
1. Check Oracle Simphony BI API documentation
2. Verify credentials and configuration
3. Review error messages and API responses
4. Contact Oracle Support for API-specific issues
5. Run endpoint discovery tests to verify availability

## License

This connector is part of the Lakeflow Community Connectors project.
