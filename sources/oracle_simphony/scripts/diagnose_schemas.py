"""
Diagnostic Script for Oracle Simphony Schema Mismatches

This script captures actual API responses from the 7 endpoints with known schema
mismatches and analyzes field presence to help fix schema definitions.

Usage:
    python sources/oracle_simphony/scripts/diagnose_schemas.py
"""

import sys
import os
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any

# Add parent directories to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from oracle_simphony import LakeflowConnect


# Define the 7 problematic endpoints
PROBLEMATIC_ENDPOINTS = {
    "order_type_daily_totals": {
        "missing_field": "ordTypeNum",
        "field_location": "nested in revenueCenters[].orderTypes[]",
        "parse_type": "keep_nested",
    },
    "guest_checks": {
        "missing_field": "busDt",
        "field_location": "root level",
        "parse_type": "flat",
    },
    "guest_check_line_item_ext_details": {
        "missing_field": "chkNum",
        "field_location": "root level",
        "parse_type": "flat",
    },
    "non_sales_transactions": {
        "missing_field": "transSeq",
        "field_location": "root level",
        "parse_type": "flat",
    },
    "pos_journal_log_details": {
        "missing_field": "logSeq",
        "field_location": "root level",
        "parse_type": "flat",
    },
    "pos_waste_details": {
        "missing_field": "wasteSeq",
        "field_location": "root level",
        "parse_type": "flat",
    },
    "location_dimensions": {
        "missing_field": "locRef",
        "field_location": "root level",
        "parse_type": "flat",
    },
}


def load_config(config_path: str) -> dict:
    """Load configuration from JSON file."""
    if not os.path.exists(config_path):
        raise FileNotFoundError(
            f"Configuration file not found: {config_path}\n"
            f"Please create the file with your Oracle Simphony credentials."
        )
    with open(config_path, "r") as f:
        return json.load(f)


def analyze_field_presence(data: Any, field_path: str) -> Dict[str, Any]:
    """
    Analyze if a field is present in the response data.

    Args:
        data: The API response data
        field_path: Path to the field (e.g., "busDt" or "revenueCenters[].orderTypes[].ordTypeNum")

    Returns:
        Dict with analysis results
    """
    result = {
        "field_present": False,
        "sample_value": None,
        "field_type": None,
        "notes": []
    }

    if not data:
        result["notes"].append("No data returned from API")
        return result

    # Handle flat structure (single level field)
    if "[]" not in field_path:
        if isinstance(data, list):
            # Check first record
            if len(data) > 0 and field_path in data[0]:
                result["field_present"] = True
                result["sample_value"] = data[0][field_path]
                result["field_type"] = type(data[0][field_path]).__name__
            else:
                result["notes"].append(f"Field '{field_path}' not found in first record")
                if len(data) > 0:
                    result["notes"].append(f"Available fields: {list(data[0].keys())}")
        elif isinstance(data, dict):
            if field_path in data:
                result["field_present"] = True
                result["sample_value"] = data[field_path]
                result["field_type"] = type(data[field_path]).__name__
            else:
                result["notes"].append(f"Field '{field_path}' not found in response")
                result["notes"].append(f"Available fields: {list(data.keys())}")

    # Handle nested structure (arrays)
    else:
        # For nested fields, we need to traverse the structure
        if isinstance(data, dict):
            # Check nested arrays
            if "revenueCenters" in data:
                rev_centers = data.get("revenueCenters", [])
                if rev_centers and len(rev_centers) > 0:
                    if "orderTypes" in rev_centers[0]:
                        order_types = rev_centers[0].get("orderTypes", [])
                        if order_types and len(order_types) > 0:
                            if "ordTypeNum" in order_types[0]:
                                result["field_present"] = True
                                result["sample_value"] = order_types[0]["ordTypeNum"]
                                result["field_type"] = type(order_types[0]["ordTypeNum"]).__name__
                            else:
                                result["notes"].append("Field 'ordTypeNum' not found in orderTypes array")
                                result["notes"].append(f"Available fields in orderTypes: {list(order_types[0].keys())}")

    return result


def diagnose_endpoint(
    connector: LakeflowConnect,
    table_name: str,
    endpoint_info: Dict[str, str],
    bus_dt: str
) -> Dict[str, Any]:
    """
    Diagnose a single endpoint by fetching data and analyzing the response.

    Args:
        connector: LakeflowConnect instance
        table_name: Name of the table
        endpoint_info: Information about the endpoint
        bus_dt: Business date to use for the request

    Returns:
        Dict with diagnosis results
    """
    print(f"\n{'='*80}")
    print(f"Diagnosing: {table_name}")
    print(f"{'='*80}")
    print(f"Missing field: {endpoint_info['missing_field']} ({endpoint_info['field_location']})")
    print(f"Parse type: {endpoint_info['parse_type']}")

    result = {
        "table_name": table_name,
        "missing_field": endpoint_info["missing_field"],
        "success": False,
        "error": None,
        "raw_response": None,
        "field_analysis": None,
        "record_count": 0,
    }

    try:
        # Get schema to see what we expect
        schema = connector.get_table_schema(table_name, {})
        print(f"\nExpected schema fields: {[f.name for f in schema.fields]}")

        # Get metadata
        metadata = connector.read_table_metadata(table_name, {})
        print(f"Primary keys: {metadata.get('primary_keys', [])}")
        print(f"Ingestion type: {metadata.get('ingestion_type', 'unknown')}")

        # Try to read data
        table_options = {}
        if table_name != "location_dimensions":  # location_dimensions doesn't use bus_dt
            table_options["bus_dt"] = bus_dt

        print(f"\nAttempting to read data with options: {table_options}")
        records_iter, next_offset = connector.read_table(
            table_name,
            start_offset={},
            table_options=table_options
        )

        # Collect records
        records = list(records_iter)
        result["record_count"] = len(records)

        if len(records) == 0:
            print("⚠️  No records returned (API returned empty result)")
            result["error"] = "No data returned from API"
            return result

        print(f"✓ Successfully fetched {len(records)} record(s)")

        # Store first few records as raw response
        result["raw_response"] = records[:3] if len(records) > 3 else records

        # Analyze field presence
        print(f"\nAnalyzing field presence for '{endpoint_info['missing_field']}'...")

        # For nested structures, pass the first record
        # For flat structures, pass the list
        if endpoint_info['parse_type'] == 'keep_nested':
            analysis_data = records[0] if records else None
        else:
            analysis_data = records

        field_analysis = analyze_field_presence(
            analysis_data,
            endpoint_info['missing_field']
        )
        result["field_analysis"] = field_analysis

        if field_analysis["field_present"]:
            print(f"✓ Field '{endpoint_info['missing_field']}' IS PRESENT in response")
            print(f"  Type: {field_analysis['field_type']}")
            print(f"  Sample value: {field_analysis['sample_value']}")
        else:
            print(f"✗ Field '{endpoint_info['missing_field']}' IS MISSING from response")
            for note in field_analysis["notes"]:
                print(f"  - {note}")

        # Show sample record structure
        print(f"\nSample record structure:")
        print(json.dumps(records[0], indent=2)[:500] + "...")

        result["success"] = True

    except Exception as e:
        print(f"✗ Error: {str(e)}")
        result["error"] = str(e)

    return result


def main():
    """Main diagnostic routine."""
    print("="*80)
    print("Oracle Simphony Schema Mismatch Diagnostic Tool")
    print("="*80)
    print()

    # Load configuration
    config_path = os.path.join(
        os.path.dirname(__file__), "../configs/dev_config.json"
    )

    try:
        config = load_config(config_path)
        print(f"✓ Loaded configuration from {config_path}")
        print(f"  Organization: {config.get('org_identifier')}")
        print(f"  Location: {config.get('loc_ref')}")
        print()
    except FileNotFoundError as e:
        print(f"✗ Error: {e}")
        sys.exit(1)

    # Initialize connector
    print("Initializing Oracle Simphony connector...")
    try:
        connector = LakeflowConnect(config)
        print("✓ Connector initialized successfully")
    except Exception as e:
        print(f"✗ Failed to initialize connector: {e}")
        sys.exit(1)

    # Use a recent business date (yesterday)
    bus_dt = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    print(f"Using business date: {bus_dt}")

    # Diagnose each endpoint
    results = []
    for table_name, endpoint_info in PROBLEMATIC_ENDPOINTS.items():
        result = diagnose_endpoint(connector, table_name, endpoint_info, bus_dt)
        results.append(result)

    # Generate summary report
    print("\n" + "="*80)
    print("SUMMARY REPORT")
    print("="*80)

    successful_diagnoses = [r for r in results if r["success"]]
    failed_diagnoses = [r for r in results if not r["success"]]

    print(f"\nSuccessfully diagnosed: {len(successful_diagnoses)}/{len(results)} endpoints")

    # Fields that are present (unexpected)
    present_fields = [
        r for r in successful_diagnoses
        if r["field_analysis"] and r["field_analysis"]["field_present"]
    ]

    # Fields that are missing (as expected)
    missing_fields = [
        r for r in successful_diagnoses
        if r["field_analysis"] and not r["field_analysis"]["field_present"]
    ]

    if present_fields:
        print(f"\n✓ Fields that ARE present (may not need schema fix):")
        for r in present_fields:
            print(f"  - {r['table_name']}.{r['missing_field']}")

    if missing_fields:
        print(f"\n✗ Fields that ARE missing (need schema fix):")
        for r in missing_fields:
            print(f"  - {r['table_name']}.{r['missing_field']}")

    if failed_diagnoses:
        print(f"\n⚠️  Failed diagnoses (no data or error):")
        for r in failed_diagnoses:
            print(f"  - {r['table_name']}: {r['error']}")

    # Save detailed results to file
    output_dir = os.path.join(os.path.dirname(__file__), "../scripts/output")
    os.makedirs(output_dir, exist_ok=True)

    output_file = os.path.join(output_dir, f"diagnosis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
    with open(output_file, "w") as f:
        json.dump(results, f, indent=2, default=str)

    print(f"\n✓ Detailed results saved to: {output_file}")

    # Recommendations
    print("\n" + "="*80)
    print("RECOMMENDATIONS")
    print("="*80)

    for r in missing_fields:
        table = r['table_name']
        field = r['missing_field']
        print(f"\n{table}:")
        print(f"  Action: Make '{field}' nullable or remove from schema")

        # Check if it's a primary key
        try:
            metadata = connector.read_table_metadata(table, {})
            pks = metadata.get('primary_keys', [])
            if field in pks:
                print(f"  ⚠️  WARNING: '{field}' is part of primary keys {pks}")
                print(f"      You'll need to adjust primary key definition")
        except:
            pass

    print("\n" + "="*80)
    print("Diagnosis complete!")
    print("="*80)


if __name__ == "__main__":
    main()
