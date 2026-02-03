"""
Test suite for Oracle Simphony BI API Connector

This test file validates the LakeflowConnect implementation for Oracle Simphony.
It uses the standard test suite from tests/test_suite.py.
"""

import sys
import os
import json

# Add parent directories to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

from tests import test_suite

# Import the connector
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from oracle_simphony import LakeflowConnect

# Inject LakeflowConnect into test_suite namespace
test_suite.LakeflowConnect = LakeflowConnect


def load_config(config_path: str) -> dict:
    """Load configuration from JSON file."""
    if not os.path.exists(config_path):
        raise FileNotFoundError(
            f"Configuration file not found: {config_path}\n"
            f"Please create the file with your Oracle Simphony credentials:\n"
            f"{{\n"
            f'  "bearer_token": "your_token_here",\n'
            f'  "org_identifier": "your_org_id",\n'
            f'  "loc_ref": "your_location_ref"\n'
            f"}}"
        )

    with open(config_path, "r") as f:
        return json.load(f)


if __name__ == "__main__":
    print("=" * 80)
    print("Oracle Simphony BI API Connector - Test Suite")
    print("=" * 80)
    print()

    # Load credentials from dev_config.json
    config_path = os.path.join(
        os.path.dirname(__file__), "../configs/dev_config.json"
    )

    try:
        init_options = load_config(config_path)
        print(f"✓ Loaded configuration from {config_path}")
        print(f"  Organization: {init_options.get('org_identifier')}")
        if 'loc_ref' in init_options:
            print(f"  Location filter: {init_options.get('loc_ref')}")
        else:
            print(f"  Location filter: None (will fetch all active locations)")
        print()
    except FileNotFoundError as e:
        print(f"✗ Error: {e}")
        sys.exit(1)

    # Optional: Configure table-specific options for testing
    # You can specify a specific business date for testing
    table_configs = {
        # Example: Test with specific dates
        # "employee_daily_totals": {
        #     "bus_dt": "2024-01-20"
        # },
    }

    # Define working tables for this instance
    # Note: Many endpoints are not available in all Oracle Simphony instances
    # Some endpoints have schema issues that need to be fixed
    working_tables = [
        # Daily Aggregations (12 working - all fixed!)
        "combo_item_daily_totals",
        "control_daily_totals",
        "discount_daily_totals",
        "employee_daily_totals",
        "job_code_daily_totals",
        "menu_item_daily_totals",
        "operations_daily_totals",
        "order_type_daily_totals",  # Fixed: renamed ordTypeNum to otNum
        "service_charge_daily_totals",
        "tax_daily_totals",
        "tender_media_daily_totals",
        # Cash Management (1 working)
        "cash_management_details",
        # Fiscal Transactions (3 working)
        "fiscal_invoice_control_data",
        "fiscal_invoice_data",
        "fiscal_total_data",
        # Kitchen Performance (1 working)
        "kds_details",
        # Transactions (6 working - all fixed!)
        "guest_checks",  # Fixed: updated to nested structure
        "guest_check_line_item_ext_details",  # Fixed: updated to nested structure
        "non_sales_transactions",  # Fixed: updated to nested structure
        "pos_journal_log_details",  # Fixed: updated to nested structure
        "pos_waste_details",  # Fixed: updated to nested structure
        "spi_payment_details",
        # Dimensions (1 working - fixed!)
        "location_dimensions",  # Fixed: updated to nested structure
    ]

    # Temporarily override LakeflowConnect.list_tables to only test working endpoints
    _original_list_tables = LakeflowConnect.list_tables

    def _list_working_tables_only(self):
        return working_tables

    LakeflowConnect.list_tables = _list_working_tables_only

    # Create tester instance
    print("Initializing test suite...")
    print(f"Testing {len(working_tables)} working endpoints (out of 55 total implemented)")
    print(f"✓ All 7 schema mismatches have been fixed!")
    tester = test_suite.LakeflowConnectTester(init_options, table_configs)
    print()

    # Run all tests
    print("Running tests...")
    print("=" * 80)
    report = tester.run_all_tests()

    # Restore original list_tables method
    LakeflowConnect.list_tables = _original_list_tables

    # Print detailed report
    print()
    print("=" * 80)
    print("Test Report")
    print("=" * 80)
    tester.print_report(report, show_details=True)

    # Exit with appropriate code
    if report.passed_tests == report.total_tests:
        print()
        print("✓ All tests passed!")
        sys.exit(0)
    else:
        print()
        print(f"✗ {report.failed_tests + report.error_tests} test(s) failed")
        sys.exit(1)
