"""
Oracle Simphony BI API Connector for LakeflowConnect

This connector provides access to Oracle Simphony Business Intelligence API endpoints
for extracting POS, payment, labor, and operational data.

API Documentation: https://docs.oracle.com/en/industries/food-beverage/back-office/20.1/biapi/
"""

import requests
import time
from datetime import datetime
from typing import Dict, List, Iterator, Any
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
    ArrayType,
    BooleanType,
)


class LakeflowConnect:
    """Oracle Simphony BI API Connector"""

    def __init__(self, options: Dict[str, str]) -> None:
        """
        Initialize Oracle Simphony BI API connector.

        Required options:
            bearer_token: Authentication token for API access
            org_identifier: Organization identifier for URL construction

        Optional options:
            loc_ref: Location reference filter (comma-separated for multiple)
                    If not provided, fetches all active locations automatically
            base_url: Override default base URL (for testing/staging)
                     Default: https://api.oracle.com/simphony/bi/v1
            timeout: Request timeout in seconds (default: 30)
        """
        # Validate required options
        required_options = ["bearer_token", "org_identifier"]
        missing_options = [opt for opt in required_options if opt not in options]
        if missing_options:
            raise ValueError(
                f"Missing required options: {', '.join(missing_options)}"
            )

        # Extract credentials and configuration
        self.bearer_token = options["bearer_token"]
        self.org_identifier = options["org_identifier"]

        # Optional location filter (can be single or comma-separated list)
        self.loc_ref_filter = options.get("loc_ref", None)
        if self.loc_ref_filter:
            self.loc_ref_filter_list = [loc.strip() for loc in self.loc_ref_filter.split(",")]
        else:
            self.loc_ref_filter_list = None

        # Cached locations (fetched on first read)
        self._locations_cache = None

        # Cached latest business dates per location (key: locRef, value: busDt string)
        self._latest_business_date_cache = {}

        self.base_url = options.get("base_url", "https://api.oracle.com/simphony/bi/v1")
        self.timeout = int(options.get("timeout", 30))

        # Construct full API base URL with org identifier
        self.api_base = f"{self.base_url}/{self.org_identifier}"

        # Setup session for connection pooling
        self._session = requests.Session()
        self._session.headers.update(
            {
                "Authorization": f"Bearer {self.bearer_token}",
                "Content-Type": "application/json",
            }
        )

        # Initialize endpoint configuration
        self._init_endpoint_config()

        # Note: Connection validation happens on first read when locations are fetched

    def _init_endpoint_config(self) -> None:
        """Initialize endpoint configuration for all 67 Oracle Simphony BI API endpoints."""
        self._endpoint_config = {
            # ============================================================
            # DAILY AGGREGATIONS (12 endpoints)
            # All require: locRef + busDt
            # ============================================================
            "combo_item_daily_totals": {
                "endpoint": "getComboItemDailyTotals",
                "parse_type": "keep_nested",
            },
            "control_daily_totals": {
                "endpoint": "getControlDailyTotals",
                "parse_type": "keep_nested",
            },
            "discount_daily_totals": {
                "endpoint": "getDiscountDailyTotals",
                "parse_type": "keep_nested",
            },
            "employee_daily_totals": {
                "endpoint": "getEmployeeDailyTotals",
                "parse_type": "keep_nested",
            },
            "job_code_daily_totals": {
                "endpoint": "getJobCodeDailyTotals",
                "parse_type": "keep_nested",
            },
            "menu_item_daily_totals": {
                "endpoint": "getMenuItemDailyTotals",
                "parse_type": "keep_nested",
            },
            "operations_daily_totals": {
                "endpoint": "getOperationsDailyTotals",
                "parse_type": "keep_nested",
            },
            "order_type_daily_totals": {
                "endpoint": "getOrderTypeDailyTotals",
                "parse_type": "keep_nested",
            },
            "revenue_center_daily_totals": {
                "endpoint": "getRevenueCenterDailyTotals",
                "parse_type": "keep_nested",
            },
            "service_charge_daily_totals": {
                "endpoint": "getServiceChargeDailyTotals",
                "parse_type": "keep_nested",
            },
            "tax_daily_totals": {
                "endpoint": "getTaxDailyTotals",
                "parse_type": "keep_nested",
            },
            "tender_media_daily_totals": {
                "endpoint": "getTenderMediaDailyTotals",
                "parse_type": "keep_nested",
            },

            # ============================================================
            # QUARTER-HOUR AGGREGATIONS (8 endpoints)
            # All require: locRef + busDt
            # ============================================================
            "combo_item_quarter_hour_totals": {
                "endpoint": "getComboItemQuarterHourTotals",
                "parse_type": "keep_nested",
            },
            "discount_quarter_hour_totals": {
                "endpoint": "getDiscountQuarterHourTotals",
                "parse_type": "keep_nested",
            },
            "job_code_quarter_hour_totals": {
                "endpoint": "getJobCodeQuarterHourTotals",
                "parse_type": "keep_nested",
            },
            "menu_item_quarter_hour_totals": {
                "endpoint": "getMenuItemQuarterHourTotals",
                "parse_type": "keep_nested",
            },
            "operations_quarter_hour_totals": {
                "endpoint": "getOperationsQuarterHourTotals",
                "parse_type": "keep_nested",
            },
            "order_type_quarter_hour_totals": {
                "endpoint": "getOrderTypeQuarterHourTotals",
                "parse_type": "keep_nested",
            },
            "service_charge_quarter_hour_totals": {
                "endpoint": "getServiceChargeQuarterHourTotals",
                "parse_type": "keep_nested",
            },
            "tax_quarter_hour_totals": {
                "endpoint": "getTaxQuarterHourTotals",
                "parse_type": "keep_nested",
            },

            # ============================================================
            # CASH MANAGEMENT (1 endpoint)
            # Requires: locRef + busDt
            # ============================================================
            "cash_management_details": {
                "endpoint": "getCashManagementDetails",
                "parse_type": "flat",
            },

            # ============================================================
            # FISCAL TRANSACTIONS (3 endpoints)
            # All require: locRef + busDt
            # ============================================================
            "fiscal_invoice_control_data": {
                "endpoint": "getFiscalInvoiceControlData",
                "parse_type": "flat",
            },
            "fiscal_invoice_data": {
                "endpoint": "getFiscalInvoiceData",
                "parse_type": "flat",
            },
            "fiscal_total_data": {
                "endpoint": "getFiscalTotalData",
                "parse_type": "flat",
            },

            # ============================================================
            # KITCHEN PERFORMANCE (1 endpoint)
            # Requires: locRef + busDt
            # ============================================================
            "kds_details": {
                "endpoint": "getKDSDetails",
                "parse_type": "flat",
            },

            # ============================================================
            # LABOR (1 endpoint)
            # Requires: locRef + busDt
            # ============================================================
            "time_card_details": {
                "endpoint": "getTimeCardDetails",
                "parse_type": "flat",
            },

            # ============================================================
            # POS DIMENSIONS (15 endpoints)
            # Most require: locRef only (no busDt)
            # Exception: location_dimensions requires neither locRef nor busDt
            # ============================================================
            "combo_meal_definitions": {
                "endpoint": "getComboMealDefinitions",
                "parse_type": "flat",
                "requires_busdt": False,
            },
            "discount_dimensions": {
                "endpoint": "getDiscounts",
                "parse_type": "flat",
                "requires_busdt": False,
            },
            "employee_dimensions": {
                "endpoint": "getEmployees",
                "parse_type": "flat",
                "requires_busdt": False,
            },
            "job_code_dimensions": {
                "endpoint": "getJobCodes",
                "parse_type": "flat",
                "requires_busdt": False,
            },
            "location_dimensions": {
                "endpoint": "getLocationDimensions",
                "parse_type": "flat",
                "requires_locref": False,
                "requires_busdt": False,
                "requires_application_name": True,
            },
            "menu_item_definitions": {
                "endpoint": "getMenuItemDefinitions",
                "parse_type": "flat",
                "requires_busdt": False,
            },
            "menu_item_dimensions": {
                "endpoint": "getMenuItems",
                "parse_type": "flat",
                "requires_busdt": False,
            },
            "non_sales_item_definitions": {
                "endpoint": "getNonSalesItemDefinitions",
                "parse_type": "flat",
                "requires_busdt": False,
            },
            "order_type_dimensions": {
                "endpoint": "getOrderTypes",
                "parse_type": "flat",
                "requires_busdt": False,
            },
            "revenue_center_dimensions": {
                "endpoint": "getRevenueCenters",
                "parse_type": "flat",
                "requires_busdt": False,
            },
            "service_charge_dimensions": {
                "endpoint": "getServiceCharges",
                "parse_type": "flat",
                "requires_busdt": False,
            },
            "tax_dimensions": {
                "endpoint": "getTaxes",
                "parse_type": "flat",
                "requires_busdt": False,
            },
            "tender_media_dimensions": {
                "endpoint": "getTenderMedias",
                "parse_type": "flat",
                "requires_busdt": False,
            },
            "time_card_code_dimensions": {
                "endpoint": "getTimeCardCodes",
                "parse_type": "flat",
                "requires_busdt": False,
            },
            "time_period_dimensions": {
                "endpoint": "getTimePeriods",
                "parse_type": "flat",
                "requires_busdt": False,
            },

            # ============================================================
            # PAYMENT DIMENSIONS (2 endpoints)
            # Require: locRef only (no busDt)
            # ============================================================
            "payment_gateway_dimensions": {
                "endpoint": "getPaymentGateways",
                "parse_type": "flat",
                "requires_busdt": False,
            },
            "spi_payment_application_dimensions": {
                "endpoint": "getSPIPaymentApplications",
                "parse_type": "flat",
                "requires_busdt": False,
            },

            # ============================================================
            # PAYMENT TRANSACTIONS (5 endpoints)
            # All require: locRef + busDt
            # ============================================================
            "payment_details": {
                "endpoint": "getPaymentDetails",
                "parse_type": "flat",
            },
            "payment_status_changes": {
                "endpoint": "getPaymentStatusChanges",
                "parse_type": "flat",
            },
            "payment_tokens": {
                "endpoint": "getPaymentTokens",
                "parse_type": "flat",
            },
            "refund_details": {
                "endpoint": "getRefundDetails",
                "parse_type": "flat",
            },
            "tip_adjustment_details": {
                "endpoint": "getTipAdjustmentDetails",
                "parse_type": "flat",
            },

            # ============================================================
            # TRANSACTIONS (7 endpoints)
            # All require: locRef + busDt
            # ============================================================
            "guest_checks": {
                "endpoint": "getGuestChecks",
                "parse_type": "flat",
            },
            "guest_check_line_item_ext_details": {
                "endpoint": "getGuestCheckLineItemExtDetails",
                "parse_type": "flat",
            },
            "guest_check_line_items": {
                "endpoint": "getGuestCheckLineItems",
                "parse_type": "flat",
            },
            "non_sales_transactions": {
                "endpoint": "getNonSalesTransactions",
                "parse_type": "flat",
            },
            "pos_journal_log_details": {
                "endpoint": "getPOSJournalLogDetails",
                "parse_type": "flat",
            },
            "pos_waste_details": {
                "endpoint": "getPOSWasteDetails",
                "parse_type": "flat",
            },
            "spi_payment_details": {
                "endpoint": "getSPIPaymentDetails",
                "parse_type": "flat",
                "blocks_application_name": True,
            },
        }

    def _fetch_and_cache_locations(self) -> List[dict]:
        """
        Fetch all locations from location_dimensions API (cached).

        Returns:
            List of location dicts with keys: locRef, name, active
        """
        if self._locations_cache is not None:
            return self._locations_cache

        print("[INFO] Fetching locations from location_dimensions API...")

        # Build request for location_dimensions
        request_body = {"applicationName": "LakeflowConnect"}
        endpoint = "getLocationDimensions"
        url = f"{self.api_base}/{endpoint}"

        try:
            response = self._session.post(url, json=request_body, timeout=self.timeout)
            self._handle_response_errors(response)
            data = response.json()

            # Parse nested response: {"curUTC": "...", "locations": [...]}
            locations_list = data.get("locations", [])

            # Apply filters
            if self.loc_ref_filter_list:
                # User specified locations - filter to those
                locations_list = [
                    loc for loc in locations_list
                    if loc.get("locRef") in self.loc_ref_filter_list
                ]
                print(f"[INFO] Filtered to {len(locations_list)} location(s): {self.loc_ref_filter_list}")
            else:
                # No filter - use only ACTIVE locations (per user requirement)
                locations_list = [loc for loc in locations_list if loc.get("active", False)]
                print(f"[INFO] Found {len(locations_list)} active locations")

            if not locations_list:
                raise ValueError("No locations found matching filter criteria")

            # Cache and return
            self._locations_cache = locations_list
            for loc in locations_list:
                print(f"  - Location {loc.get('locRef')}: {loc.get('name', 'Unknown')}")

            return self._locations_cache

        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"Failed to fetch locations: {str(e)}")

    def _fetch_latest_business_date_for_location(self, loc_ref: str) -> str:
        """
        Fetch the latest available business date for a specific location (cached per location).

        Args:
            loc_ref: Location reference to get the latest business date for

        Returns:
            Date string in "YYYY-MM-DD" format

        Raises:
            RuntimeError: If the API call fails
        """
        # Check cache first
        if loc_ref in self._latest_business_date_cache:
            return self._latest_business_date_cache[loc_ref]

        print(f"[INFO] Fetching latest business date for location {loc_ref}...")

        # Build request for getLatestBusDt endpoint
        request_body = {"locRef": loc_ref}
        endpoint = "getLatestBusDt"
        url = f"{self.api_base}/{endpoint}"

        try:
            response = self._session.post(url, json=request_body, timeout=self.timeout)
            self._handle_response_errors(response)
            data = response.json()

            # Extract business date from response
            # The API returns: {"curUTC": "...", "locRef": "29", "latestBusDt": "2024-01-30", "softwareVersion": "..."}
            latest_date = data.get("latestBusDt")

            if not latest_date:
                raise ValueError(f"No latestBusDt field returned for location {loc_ref}. Response: {data}")

            # Cache and return
            self._latest_business_date_cache[loc_ref] = latest_date
            print(f"[INFO] Latest business date for location {loc_ref}: {latest_date}")

            return latest_date

        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"Failed to fetch latest business date for location {loc_ref}: {str(e)}")

    def _validate_connection(self) -> None:
        """Validate credentials by making a test API call."""
        try:
            # Use a lightweight endpoint for validation
            test_body = {
                "locRef": self.loc_ref,
                "busDt": datetime.now().strftime("%Y-%m-%d"),
            }

            url = f"{self.api_base}/getOperationsDailyTotals"
            print(f"[DEBUG] Validating connection: POST {url}")
            print(f"[DEBUG] Request body: {test_body}")
            response = self._session.post(url, json=test_body, timeout=self.timeout)
            print(f"[DEBUG] Validation response status: {response.status_code}")

            # Accept both 200 (success) and 404 (no data) as valid auth
            if response.status_code not in [200, 404]:
                self._handle_response_errors(response)

        except requests.exceptions.RequestException as e:
            raise RuntimeError(
                f"Failed to validate Oracle Simphony connection: {str(e)}"
            )

    def _handle_response_errors(self, response: requests.Response) -> None:
        """Handle API error responses."""
        if response.status_code == 200:
            return

        error_messages = {
            400: "Bad Request: Invalid request parameters",
            401: "Unauthorized: Invalid bearer token",
            403: "Forbidden: Insufficient permissions for this endpoint",
            404: "Not Found: Endpoint or resource does not exist",
            429: "Rate Limited: Too many requests",
            500: "Internal Server Error: API service error",
            503: "Service Unavailable: API temporarily unavailable",
        }

        error_msg = error_messages.get(
            response.status_code, f"HTTP {response.status_code}"
        )

        # Try to extract detailed error message from response
        try:
            error_data = response.json()
            if isinstance(error_data, dict):
                detail = error_data.get("detail") or error_data.get("message", "")
                if detail:
                    error_msg += f": {detail}"
        except:
            pass

        raise RuntimeError(
            f"Oracle Simphony API error: {error_msg}\n"
            f"Response: {response.text[:500]}"
        )

    def list_tables(self) -> List[str]:
        """Return list of all 67 Oracle Simphony BI API tables."""
        return [
            # Daily Aggregations (12 endpoints)
            "combo_item_daily_totals",
            "control_daily_totals",
            "discount_daily_totals",
            "employee_daily_totals",
            "job_code_daily_totals",
            "menu_item_daily_totals",
            "operations_daily_totals",
            "order_type_daily_totals",
            "revenue_center_daily_totals",
            "service_charge_daily_totals",
            "tax_daily_totals",
            "tender_media_daily_totals",

            # Quarter-Hour Aggregations (8 endpoints)
            "combo_item_quarter_hour_totals",
            "discount_quarter_hour_totals",
            "job_code_quarter_hour_totals",
            "menu_item_quarter_hour_totals",
            "operations_quarter_hour_totals",
            "order_type_quarter_hour_totals",
            "service_charge_quarter_hour_totals",
            "tax_quarter_hour_totals",

            # Cash Management (1 endpoint)
            "cash_management_details",

            # Fiscal Transactions (3 endpoints)
            "fiscal_invoice_control_data",
            "fiscal_invoice_data",
            "fiscal_total_data",

            # Kitchen Performance (1 endpoint)
            "kds_details",

            # Labor (1 endpoint)
            "time_card_details",

            # POS Dimensions (15 endpoints)
            "combo_meal_definitions",
            "discount_dimensions",
            "employee_dimensions",
            "job_code_dimensions",
            "location_dimensions",
            "menu_item_definitions",
            "menu_item_dimensions",
            "non_sales_item_definitions",
            "order_type_dimensions",
            "revenue_center_dimensions",
            "service_charge_dimensions",
            "tax_dimensions",
            "tender_media_dimensions",
            "time_card_code_dimensions",
            "time_period_dimensions",

            # Payment Dimensions (2 endpoints)
            "payment_gateway_dimensions",
            "spi_payment_application_dimensions",

            # Payment Transactions (5 endpoints)
            "payment_details",
            "payment_status_changes",
            "payment_tokens",
            "refund_details",
            "tip_adjustment_details",

            # Transactions (7 endpoints)
            "guest_checks",
            "guest_check_line_item_ext_details",
            "guest_check_line_items",
            "non_sales_transactions",
            "pos_journal_log_details",
            "pos_waste_details",
            "spi_payment_details",
        ]

    def get_table_schema(
        self, table_name: str, table_options: Dict[str, str]
    ) -> StructType:
        """Return schema for the specified table."""
        schema_factory_map = {
            # Daily Aggregations (12)
            "combo_item_daily_totals": self._get_combo_item_daily_totals_schema,
            "control_daily_totals": self._get_control_daily_totals_schema,
            "discount_daily_totals": self._get_discount_daily_totals_schema,
            "employee_daily_totals": self._get_employee_daily_totals_schema,
            "job_code_daily_totals": self._get_job_code_daily_totals_schema,
            "menu_item_daily_totals": self._get_menu_item_daily_totals_schema,
            "operations_daily_totals": self._get_operations_daily_totals_schema,
            "order_type_daily_totals": self._get_order_type_daily_totals_schema,
            "revenue_center_daily_totals": self._get_revenue_center_daily_totals_schema,
            "service_charge_daily_totals": self._get_service_charge_daily_totals_schema,
            "tax_daily_totals": self._get_tax_daily_totals_schema,
            "tender_media_daily_totals": self._get_tender_media_daily_totals_schema,

            # Quarter-Hour Aggregations (8)
            "combo_item_quarter_hour_totals": self._get_combo_item_quarter_hour_totals_schema,
            "discount_quarter_hour_totals": self._get_discount_quarter_hour_totals_schema,
            "job_code_quarter_hour_totals": self._get_job_code_quarter_hour_totals_schema,
            "menu_item_quarter_hour_totals": self._get_menu_item_quarter_hour_totals_schema,
            "operations_quarter_hour_totals": self._get_operations_quarter_hour_totals_schema,
            "order_type_quarter_hour_totals": self._get_order_type_quarter_hour_totals_schema,
            "service_charge_quarter_hour_totals": self._get_service_charge_quarter_hour_totals_schema,
            "tax_quarter_hour_totals": self._get_tax_quarter_hour_totals_schema,

            # Cash Management (1)
            "cash_management_details": self._get_cash_management_details_schema,

            # Fiscal Transactions (3)
            "fiscal_invoice_control_data": self._get_fiscal_invoice_control_data_schema,
            "fiscal_invoice_data": self._get_fiscal_invoice_data_schema,
            "fiscal_total_data": self._get_fiscal_total_data_schema,

            # Kitchen Performance & Labor (2)
            "kds_details": self._get_kds_details_schema,
            "time_card_details": self._get_time_card_details_schema,

            # POS Dimensions (15)
            "combo_meal_definitions": self._get_combo_meal_definitions_schema,
            "discount_dimensions": self._get_discount_dimensions_schema,
            "employee_dimensions": self._get_employee_dimensions_schema,
            "job_code_dimensions": self._get_job_code_dimensions_schema,
            "location_dimensions": self._get_location_dimensions_schema,
            "menu_item_definitions": self._get_menu_item_definitions_schema,
            "menu_item_dimensions": self._get_menu_item_dimensions_schema,
            "non_sales_item_definitions": self._get_non_sales_item_definitions_schema,
            "order_type_dimensions": self._get_order_type_dimensions_schema,
            "revenue_center_dimensions": self._get_revenue_center_dimensions_schema,
            "service_charge_dimensions": self._get_service_charge_dimensions_schema,
            "tax_dimensions": self._get_tax_dimensions_schema,
            "tender_media_dimensions": self._get_tender_media_dimensions_schema,
            "time_card_code_dimensions": self._get_time_card_code_dimensions_schema,
            "time_period_dimensions": self._get_time_period_dimensions_schema,

            # Payment Dimensions (2)
            "payment_gateway_dimensions": self._get_payment_gateway_dimensions_schema,
            "spi_payment_application_dimensions": self._get_spi_payment_application_dimensions_schema,

            # Payment Transactions (5)
            "payment_details": self._get_payment_details_schema,
            "payment_status_changes": self._get_payment_status_changes_schema,
            "payment_tokens": self._get_payment_tokens_schema,
            "refund_details": self._get_refund_details_schema,
            "tip_adjustment_details": self._get_tip_adjustment_details_schema,

            # Transactions (7)
            "guest_checks": self._get_guest_checks_schema,
            "guest_check_line_item_ext_details": self._get_guest_check_line_item_ext_details_schema,
            "guest_check_line_items": self._get_guest_check_line_items_schema,
            "non_sales_transactions": self._get_non_sales_transactions_schema,
            "pos_journal_log_details": self._get_pos_journal_log_details_schema,
            "pos_waste_details": self._get_pos_waste_details_schema,
            "spi_payment_details": self._get_spi_payment_details_schema,
        }

        if table_name not in schema_factory_map:
            raise ValueError(f"Unsupported table: {table_name}")

        return schema_factory_map[table_name]()

    def _get_employee_daily_totals_schema(self) -> StructType:
        """Schema for employee daily totals with nested structure."""
        return StructType(
            [
                StructField("locRef", StringType(), True),
                StructField("busDt", StringType(), True),
                StructField(
                    "revenueCenters",
                    ArrayType(
                        StructType(
                            [
                                StructField("rvcNum", LongType(), False),
                                StructField(
                                    "employees",
                                    ArrayType(
                                        StructType(
                                            [
                                                StructField("empNum", LongType(), False),
                                                StructField(
                                                    "netSlsTtl", DoubleType(), True
                                                ),
                                                StructField(
                                                    "itmDscTtl", DoubleType(), True
                                                ),
                                                StructField(
                                                    "subDscTtl", DoubleType(), True
                                                ),
                                                StructField("svcTtl", DoubleType(), True),
                                                StructField(
                                                    "nonRevSvcTtl", DoubleType(), True
                                                ),
                                                StructField("rtnCnt", LongType(), True),
                                                StructField("rtnTtl", DoubleType(), True),
                                                StructField(
                                                    "credTtl", DoubleType(), True
                                                ),
                                                StructField("rndTtl", DoubleType(), True),
                                                StructField(
                                                    "chngInGrndTtl", DoubleType(), True
                                                ),
                                                StructField(
                                                    "tblTurnCnt", LongType(), True
                                                ),
                                                StructField(
                                                    "dineTimeInMins", DoubleType(), True
                                                ),
                                                StructField("chkCnt", LongType(), True),
                                                StructField("gstCnt", LongType(), True),
                                                StructField("vdTtl", DoubleType(), True),
                                                StructField("vdCnt", LongType(), True),
                                                StructField(
                                                    "errCorTtl", DoubleType(), True
                                                ),
                                                StructField(
                                                    "errCorCnt", LongType(), True
                                                ),
                                                StructField(
                                                    "mngrVdTtl", DoubleType(), True
                                                ),
                                                StructField(
                                                    "mngrVdCnt", LongType(), True
                                                ),
                                                StructField(
                                                    "transCnclTtl", DoubleType(), True
                                                ),
                                                StructField(
                                                    "transCnclCnt", LongType(), True
                                                ),
                                                StructField(
                                                    "carryoverTtl", DoubleType(), True
                                                ),
                                                StructField(
                                                    "carryoverCnt", LongType(), True
                                                ),
                                                StructField(
                                                    "chkOpnTtl", DoubleType(), True
                                                ),
                                                StructField(
                                                    "chkOpnCnt", LongType(), True
                                                ),
                                                StructField(
                                                    "chkClsdTtl", DoubleType(), True
                                                ),
                                                StructField(
                                                    "chkClsdCnt", LongType(), True
                                                ),
                                                StructField(
                                                    "chkXferInTtl", DoubleType(), True
                                                ),
                                                StructField(
                                                    "chkXferInCnt", LongType(), True
                                                ),
                                                StructField(
                                                    "chkXferOutTtl", DoubleType(), True
                                                ),
                                                StructField(
                                                    "chkXferOutCnt", LongType(), True
                                                ),
                                                StructField(
                                                    "noSalesCnt", LongType(), True
                                                ),
                                                StructField(
                                                    "grossRcptsTtl", DoubleType(), True
                                                ),
                                                StructField(
                                                    "chrgRcptsTtl", DoubleType(), True
                                                ),
                                                StructField(
                                                    "chrgTipsTtl", DoubleType(), True
                                                ),
                                                StructField(
                                                    "indirTipsPdTtl", DoubleType(), True
                                                ),
                                                StructField(
                                                    "tipsDclrdTtl", DoubleType(), True
                                                ),
                                                StructField(
                                                    "tipsPdTtl", DoubleType(), True
                                                ),
                                                StructField("tipTtl", DoubleType(), True),
                                                StructField("taxTtl", DoubleType(), True),
                                                StructField(
                                                    "tipsOutTtl", DoubleType(), True
                                                ),
                                            ]
                                        )
                                    ),
                                    True,
                                ),
                            ]
                        )
                    ),
                    True,
                ),
            ]
        )

    def _get_menu_item_daily_totals_schema(self) -> StructType:
        """Schema for menu item daily totals with nested structure."""
        return StructType(
            [
                StructField("locRef", StringType(), True),
                StructField("busDt", StringType(), True),
                StructField(
                    "revenueCenters",
                    ArrayType(
                        StructType(
                            [
                                StructField("rvcNum", LongType(), False),
                                StructField(
                                    "menuItems",
                                    ArrayType(
                                        StructType(
                                            [
                                                StructField("miNum", LongType(), False),
                                                StructField(
                                                    "netSlsTtl", DoubleType(), True
                                                ),
                                                StructField(
                                                    "grsSlsTtl", DoubleType(), True
                                                ),
                                                StructField("slsCnt", LongType(), True),
                                                StructField(
                                                    "rtnCnt", LongType(), True
                                                ),
                                                StructField(
                                                    "rtnTtl", DoubleType(), True
                                                ),
                                                StructField("vdCnt", LongType(), True),
                                                StructField("vdTtl", DoubleType(), True),
                                            ]
                                        )
                                    ),
                                    True,
                                ),
                            ]
                        )
                    ),
                    True,
                ),
            ]
        )

    def _get_operations_daily_totals_schema(self) -> StructType:
        """Schema for operations daily totals with nested structure."""
        return StructType(
            [
                StructField("locRef", StringType(), True),
                StructField("busDt", StringType(), True),
                StructField(
                    "revenueCenters",
                    ArrayType(
                        StructType(
                            [
                                StructField("rvcNum", LongType(), False),
                                StructField("netSlsTtl", DoubleType(), True),
                                StructField("grsSlsTtl", DoubleType(), True),
                                StructField("chkCnt", LongType(), True),
                                StructField("gstCnt", LongType(), True),
                                StructField("svcChgTtl", DoubleType(), True),
                                StructField("dscTtl", DoubleType(), True),
                                StructField("taxTtl", DoubleType(), True),
                            ]
                        )
                    ),
                    True,
                ),
            ]
        )

    def _get_discount_daily_totals_schema(self) -> StructType:
        """Schema for discount daily totals with nested structure."""
        return StructType(
            [
                StructField("locRef", StringType(), True),
                StructField("busDt", StringType(), True),
                StructField(
                    "revenueCenters",
                    ArrayType(
                        StructType(
                            [
                                StructField("rvcNum", LongType(), False),
                                StructField(
                                    "discounts",
                                    ArrayType(
                                        StructType(
                                            [
                                                StructField("dscNum", LongType(), False),
                                                StructField(
                                                    "dscTtl", DoubleType(), True
                                                ),
                                                StructField("dscCnt", LongType(), True),
                                            ]
                                        )
                                    ),
                                    True,
                                ),
                            ]
                        )
                    ),
                    True,
                ),
            ]
        )

    def _get_tender_media_daily_totals_schema(self) -> StructType:
        """Schema for tender media daily totals with nested structure."""
        return StructType(
            [
                StructField("locRef", StringType(), True),
                StructField("busDt", StringType(), True),
                StructField(
                    "revenueCenters",
                    ArrayType(
                        StructType(
                            [
                                StructField("rvcNum", LongType(), False),
                                StructField(
                                    "tenderMedia",
                                    ArrayType(
                                        StructType(
                                            [
                                                StructField("tndNum", LongType(), False),
                                                StructField(
                                                    "tndTtl", DoubleType(), True
                                                ),
                                                StructField("tndCnt", LongType(), True),
                                            ]
                                        )
                                    ),
                                    True,
                                ),
                            ]
                        )
                    ),
                    True,
                ),
            ]
        )

    def _get_menu_item_quarter_hour_totals_schema(self) -> StructType:
        """Schema for menu item quarter hour totals with nested structure."""
        return StructType(
            [
                StructField("locRef", StringType(), True),
                StructField("busDt", StringType(), True),
                StructField(
                    "revenueCenters",
                    ArrayType(
                        StructType(
                            [
                                StructField("rvcNum", LongType(), False),
                                StructField(
                                    "quarters",
                                    ArrayType(
                                        StructType(
                                            [
                                                StructField(
                                                    "quarterHour", LongType(), False
                                                ),
                                                StructField(
                                                    "menuItems",
                                                    ArrayType(
                                                        StructType(
                                                            [
                                                                StructField(
                                                                    "miNum",
                                                                    LongType(),
                                                                    False,
                                                                ),
                                                                StructField(
                                                                    "netSlsTtl",
                                                                    DoubleType(),
                                                                    True,
                                                                ),
                                                                StructField(
                                                                    "grsSlsTtl",
                                                                    DoubleType(),
                                                                    True,
                                                                ),
                                                                StructField(
                                                                    "slsCnt",
                                                                    LongType(),
                                                                    True,
                                                                ),
                                                            ]
                                                        )
                                                    ),
                                                    True,
                                                ),
                                            ]
                                        )
                                    ),
                                    True,
                                ),
                            ]
                        )
                    ),
                    True,
                ),
            ]
        )

    def _get_cash_management_details_schema(self) -> StructType:
        """Schema for cash management details."""
        return StructType(
            [
                StructField("locRef", StringType(), False),
                StructField("busDt", StringType(), False),
                StructField("rvcNum", LongType(), True),
                StructField("empNum", LongType(), True),
                StructField("wsNum", LongType(), True),
                StructField("transSeq", LongType(), True),
                StructField("transDt", StringType(), True),
                StructField("transTm", StringType(), True),
                StructField("itemNum", LongType(), True),
                StructField("itemAmt", DoubleType(), True),
                StructField("itemQty", LongType(), True),
            ]
        )

    def _get_transaction_headers_schema(self) -> StructType:
        """Schema for transaction headers (guest checks)."""
        return StructType(
            [
                StructField("locRef", StringType(), False),
                StructField("busDt", StringType(), False),
                StructField("chkNum", LongType(), False),
                StructField("rvcNum", LongType(), True),
                StructField("empNum", LongType(), True),
                StructField("tblNum", LongType(), True),
                StructField("gstCnt", LongType(), True),
                StructField("subTtl", DoubleType(), True),
                StructField("taxTtl", DoubleType(), True),
                StructField("chkTtl", DoubleType(), True),
                StructField("openDtTm", StringType(), True),
                StructField("closeDtTm", StringType(), True),
            ]
        )

    def _get_transaction_items_schema(self) -> StructType:
        """Schema for transaction items (guest check line items)."""
        return StructType(
            [
                StructField("locRef", StringType(), False),
                StructField("busDt", StringType(), False),
                StructField("chkNum", LongType(), False),
                StructField("dtlSeq", LongType(), False),
                StructField("miNum", LongType(), True),
                StructField("miName", StringType(), True),
                StructField("qty", LongType(), True),
                StructField("price", DoubleType(), True),
                StructField("amount", DoubleType(), True),
            ]
        )

    def _get_transaction_tenders_schema(self) -> StructType:
        """Schema for transaction tenders (SPI payment details)."""
        return StructType(
            [
                StructField("locRef", StringType(), False),
                StructField("busDt", StringType(), False),
                StructField("chkNum", LongType(), False),
                StructField("tndNum", LongType(), True),
                StructField("tndTtl", DoubleType(), True),
                StructField("cardType", StringType(), True),
                StructField("last4", StringType(), True),
                StructField("authCode", StringType(), True),
            ]
        )

    def _get_pos_employees_schema(self) -> StructType:
        """Schema for POS employees dimension."""
        return StructType(
            [
                StructField("locRef", StringType(), False),
                StructField("empNum", LongType(), False),
                StructField("firstName", StringType(), True),
                StructField("lastName", StringType(), True),
                StructField("jobCode", LongType(), True),
            ]
        )

    def _get_pos_menu_items_schema(self) -> StructType:
        """Schema for POS menu items dimension."""
        return StructType(
            [
                StructField("locRef", StringType(), False),
                StructField("miNum", LongType(), False),
                StructField("miName", StringType(), True),
                StructField("miClass", LongType(), True),
                StructField("miFamily", LongType(), True),
                StructField("price", DoubleType(), True),
            ]
        )

    def _get_pos_revenue_centers_schema(self) -> StructType:
        """Schema for POS revenue centers dimension."""
        return StructType(
            [
                StructField("locRef", StringType(), False),
                StructField("rvcNum", LongType(), False),
                StructField("rvcName", StringType(), True),
            ]
        )

    def _get_pos_locations_schema(self) -> StructType:
        """Schema for POS locations dimension."""
        return StructType(
            [
                StructField("locRef", StringType(), False),
                StructField("locName", StringType(), True),
                StructField("address", StringType(), True),
                StructField("city", StringType(), True),
                StructField("state", StringType(), True),
                StructField("zip", StringType(), True),
            ]
        )

    def _get_payment_transactions_schema(self) -> StructType:
        """Schema for payment transactions."""
        return StructType(
            [
                StructField("locRef", StringType(), False),
                StructField("busDt", StringType(), False),
                StructField("transId", StringType(), False),
                StructField("transType", StringType(), True),
                StructField("transAmt", DoubleType(), True),
                StructField("transDtTm", StringType(), True),
                StructField("status", StringType(), True),
            ]
        )

    # ============================================================
    # NEW DAILY AGGREGATION SCHEMAS
    # ============================================================

    def _get_combo_item_daily_totals_schema(self) -> StructType:
        """Schema for combo item daily totals."""
        return StructType([
            StructField("locRef", StringType(), True),
            StructField("busDt", StringType(), True),
            StructField("revenueCenters", ArrayType(StructType([
                StructField("rvcNum", LongType(), False),
                StructField("comboItems", ArrayType(StructType([
                    StructField("comboNum", LongType(), False),
                    StructField("netSlsTtl", DoubleType(), True),
                    StructField("grsSlsTtl", DoubleType(), True),
                    StructField("slsCnt", LongType(), True),
                ])), True),
            ])), True),
        ])

    def _get_control_daily_totals_schema(self) -> StructType:
        """Schema for control daily totals."""
        return StructType([
            StructField("locRef", StringType(), True),
            StructField("busDt", StringType(), True),
            StructField("revenueCenters", ArrayType(StructType([
                StructField("rvcNum", LongType(), False),
                StructField("netSlsTtl", DoubleType(), True),
                StructField("grsSlsTtl", DoubleType(), True),
                StructField("chkCnt", LongType(), True),
                StructField("gstCnt", LongType(), True),
            ])), True),
        ])

    def _get_job_code_daily_totals_schema(self) -> StructType:
        """Schema for job code daily totals."""
        return StructType([
            StructField("locRef", StringType(), True),
            StructField("busDt", StringType(), True),
            StructField("revenueCenters", ArrayType(StructType([
                StructField("rvcNum", LongType(), False),
                StructField("jobCodes", ArrayType(StructType([
                    StructField("jobCode", LongType(), False),
                    StructField("netSlsTtl", DoubleType(), True),
                    StructField("chkCnt", LongType(), True),
                ])), True),
            ])), True),
        ])

    def _get_order_type_daily_totals_schema(self) -> StructType:
        """Schema for order type daily totals."""
        return StructType([
            StructField("locRef", StringType(), True),
            StructField("busDt", StringType(), True),
            StructField("revenueCenters", ArrayType(StructType([
                StructField("rvcNum", LongType(), False),
                StructField("orderTypes", ArrayType(StructType([
                    StructField("otNum", LongType(), False),  # Fixed: was ordTypeNum
                    StructField("netSlsTtl", DoubleType(), True),
                    StructField("chkCnt", LongType(), True),
                    StructField("gstCnt", LongType(), True),
                    StructField("dscTtl", DoubleType(), True),
                    StructField("svcTtl", DoubleType(), True),
                    StructField("taxColTtl", DoubleType(), True),
                    StructField("tblTurnCnt", LongType(), True),
                    StructField("dineTimeInMins", DoubleType(), True),
                    StructField("vdTtl", DoubleType(), True),
                    StructField("vdCnt", LongType(), True),
                    StructField("errCorTtl", DoubleType(), True),
                    StructField("errCorCnt", LongType(), True),
                    StructField("mngrVdTtl", DoubleType(), True),
                    StructField("mngrVdCnt", LongType(), True),
                    StructField("chkOpnCnt", LongType(), True),
                    StructField("chkClsdCnt", LongType(), True),
                ])), True),
            ])), True),
        ])

    def _get_revenue_center_daily_totals_schema(self) -> StructType:
        """Schema for revenue center daily totals."""
        return StructType([
            StructField("locRef", StringType(), True),
            StructField("busDt", StringType(), True),
            StructField("revenueCenters", ArrayType(StructType([
                StructField("rvcNum", LongType(), False),
                StructField("netSlsTtl", DoubleType(), True),
                StructField("grsSlsTtl", DoubleType(), True),
                StructField("chkCnt", LongType(), True),
            ])), True),
        ])

    def _get_service_charge_daily_totals_schema(self) -> StructType:
        """Schema for service charge daily totals."""
        return StructType([
            StructField("locRef", StringType(), True),
            StructField("busDt", StringType(), True),
            StructField("revenueCenters", ArrayType(StructType([
                StructField("rvcNum", LongType(), False),
                StructField("serviceCharges", ArrayType(StructType([
                    StructField("svcNum", LongType(), False),
                    StructField("svcTtl", DoubleType(), True),
                    StructField("svcCnt", LongType(), True),
                ])), True),
            ])), True),
        ])

    def _get_tax_daily_totals_schema(self) -> StructType:
        """Schema for tax daily totals."""
        return StructType([
            StructField("locRef", StringType(), True),
            StructField("busDt", StringType(), True),
            StructField("revenueCenters", ArrayType(StructType([
                StructField("rvcNum", LongType(), False),
                StructField("taxes", ArrayType(StructType([
                    StructField("taxNum", LongType(), False),
                    StructField("taxTtl", DoubleType(), True),
                    StructField("taxCnt", LongType(), True),
                ])), True),
            ])), True),
        ])

    # ============================================================
    # QUARTER-HOUR AGGREGATION SCHEMAS
    # ============================================================

    def _get_combo_item_quarter_hour_totals_schema(self) -> StructType:
        """Schema for combo item quarter hour totals."""
        return StructType([
            StructField("locRef", StringType(), True),
            StructField("busDt", StringType(), True),
            StructField("revenueCenters", ArrayType(StructType([
                StructField("rvcNum", LongType(), False),
                StructField("quarters", ArrayType(StructType([
                    StructField("quarterHour", LongType(), False),
                    StructField("comboItems", ArrayType(StructType([
                        StructField("comboNum", LongType(), False),
                        StructField("netSlsTtl", DoubleType(), True),
                    ])), True),
                ])), True),
            ])), True),
        ])

    def _get_discount_quarter_hour_totals_schema(self) -> StructType:
        """Schema for discount quarter hour totals."""
        return StructType([
            StructField("locRef", StringType(), True),
            StructField("busDt", StringType(), True),
            StructField("revenueCenters", ArrayType(StructType([
                StructField("rvcNum", LongType(), False),
                StructField("quarters", ArrayType(StructType([
                    StructField("quarterHour", LongType(), False),
                    StructField("discounts", ArrayType(StructType([
                        StructField("dscNum", LongType(), False),
                        StructField("dscTtl", DoubleType(), True),
                    ])), True),
                ])), True),
            ])), True),
        ])

    def _get_job_code_quarter_hour_totals_schema(self) -> StructType:
        """Schema for job code quarter hour totals."""
        return StructType([
            StructField("locRef", StringType(), True),
            StructField("busDt", StringType(), True),
            StructField("revenueCenters", ArrayType(StructType([
                StructField("rvcNum", LongType(), False),
                StructField("quarters", ArrayType(StructType([
                    StructField("quarterHour", LongType(), False),
                    StructField("jobCodes", ArrayType(StructType([
                        StructField("jobCode", LongType(), False),
                        StructField("netSlsTtl", DoubleType(), True),
                    ])), True),
                ])), True),
            ])), True),
        ])

    def _get_operations_quarter_hour_totals_schema(self) -> StructType:
        """Schema for operations quarter hour totals."""
        return StructType([
            StructField("locRef", StringType(), True),
            StructField("busDt", StringType(), True),
            StructField("revenueCenters", ArrayType(StructType([
                StructField("rvcNum", LongType(), False),
                StructField("quarters", ArrayType(StructType([
                    StructField("quarterHour", LongType(), False),
                    StructField("netSlsTtl", DoubleType(), True),
                    StructField("chkCnt", LongType(), True),
                ])), True),
            ])), True),
        ])

    def _get_order_type_quarter_hour_totals_schema(self) -> StructType:
        """Schema for order type quarter hour totals."""
        return StructType([
            StructField("locRef", StringType(), True),
            StructField("busDt", StringType(), True),
            StructField("revenueCenters", ArrayType(StructType([
                StructField("rvcNum", LongType(), False),
                StructField("quarters", ArrayType(StructType([
                    StructField("quarterHour", LongType(), False),
                    StructField("orderTypes", ArrayType(StructType([
                        StructField("ordTypeNum", LongType(), False),
                        StructField("netSlsTtl", DoubleType(), True),
                    ])), True),
                ])), True),
            ])), True),
        ])

    def _get_service_charge_quarter_hour_totals_schema(self) -> StructType:
        """Schema for service charge quarter hour totals."""
        return StructType([
            StructField("locRef", StringType(), True),
            StructField("busDt", StringType(), True),
            StructField("revenueCenters", ArrayType(StructType([
                StructField("rvcNum", LongType(), False),
                StructField("quarters", ArrayType(StructType([
                    StructField("quarterHour", LongType(), False),
                    StructField("serviceCharges", ArrayType(StructType([
                        StructField("svcNum", LongType(), False),
                        StructField("svcTtl", DoubleType(), True),
                    ])), True),
                ])), True),
            ])), True),
        ])

    def _get_tax_quarter_hour_totals_schema(self) -> StructType:
        """Schema for tax quarter hour totals."""
        return StructType([
            StructField("locRef", StringType(), True),
            StructField("busDt", StringType(), True),
            StructField("revenueCenters", ArrayType(StructType([
                StructField("rvcNum", LongType(), False),
                StructField("quarters", ArrayType(StructType([
                    StructField("quarterHour", LongType(), False),
                    StructField("taxes", ArrayType(StructType([
                        StructField("taxNum", LongType(), False),
                        StructField("taxTtl", DoubleType(), True),
                    ])), True),
                ])), True),
            ])), True),
        ])

    # ============================================================
    # FISCAL TRANSACTION SCHEMAS
    # ============================================================

    def _get_fiscal_invoice_control_data_schema(self) -> StructType:
        """Schema for fiscal invoice control data."""
        return StructType([
            StructField("locRef", StringType(), False),
            StructField("busDt", StringType(), False),
            StructField("invoiceNum", StringType(), True),
            StructField("rvcNum", LongType(), True),
            StructField("chkNum", LongType(), True),
            StructField("totalAmt", DoubleType(), True),
        ])

    def _get_fiscal_invoice_data_schema(self) -> StructType:
        """Schema for fiscal invoice data."""
        return StructType([
            StructField("locRef", StringType(), False),
            StructField("busDt", StringType(), False),
            StructField("invoiceNum", StringType(), True),
            StructField("invoiceType", StringType(), True),
            StructField("invoiceAmt", DoubleType(), True),
            StructField("invoiceDt", StringType(), True),
        ])

    def _get_fiscal_total_data_schema(self) -> StructType:
        """Schema for fiscal total data."""
        return StructType([
            StructField("locRef", StringType(), False),
            StructField("busDt", StringType(), False),
            StructField("fiscalTotalType", StringType(), True),
            StructField("totalAmt", DoubleType(), True),
        ])

    # ============================================================
    # KITCHEN PERFORMANCE & LABOR SCHEMAS
    # ============================================================

    def _get_kds_details_schema(self) -> StructType:
        """Schema for KDS (Kitchen Display System) details."""
        return StructType([
            StructField("locRef", StringType(), False),
            StructField("busDt", StringType(), False),
            StructField("chkNum", LongType(), True),
            StructField("orderDtTm", StringType(), True),
            StructField("completeDtTm", StringType(), True),
            StructField("prepTimeInSec", LongType(), True),
        ])

    def _get_time_card_details_schema(self) -> StructType:
        """Schema for time card details."""
        return StructType([
            StructField("locRef", StringType(), False),
            StructField("busDt", StringType(), False),
            StructField("empNum", LongType(), True),
            StructField("jobCode", LongType(), True),
            StructField("clockInDtTm", StringType(), True),
            StructField("clockOutDtTm", StringType(), True),
            StructField("hoursWorked", DoubleType(), True),
        ])

    # ============================================================
    # DIMENSION SCHEMAS
    # ============================================================

    def _get_combo_meal_definitions_schema(self) -> StructType:
        """Schema for combo meal definitions."""
        return StructType([
            StructField("locRef", StringType(), False),
            StructField("comboNum", LongType(), False),
            StructField("comboName", StringType(), True),
            StructField("comboPrice", DoubleType(), True),
        ])

    def _get_discount_dimensions_schema(self) -> StructType:
        """Schema for discount dimensions."""
        return StructType([
            StructField("locRef", StringType(), False),
            StructField("dscNum", LongType(), False),
            StructField("dscName", StringType(), True),
            StructField("dscType", StringType(), True),
        ])

    def _get_employee_dimensions_schema(self) -> StructType:
        """Schema for employee dimensions."""
        return StructType([
            StructField("locRef", StringType(), False),
            StructField("empNum", LongType(), False),
            StructField("firstName", StringType(), True),
            StructField("lastName", StringType(), True),
            StructField("jobCode", LongType(), True),
        ])

    def _get_job_code_dimensions_schema(self) -> StructType:
        """Schema for job code dimensions."""
        return StructType([
            StructField("locRef", StringType(), False),
            StructField("jobCode", LongType(), False),
            StructField("jobName", StringType(), True),
        ])

    def _get_location_dimensions_schema(self) -> StructType:
        """Schema for location dimensions - nested structure."""
        return StructType([
            StructField("curUTC", StringType(), True),
            StructField("locations", ArrayType(StructType([
                StructField("locRef", StringType(), False),
                StructField("name", StringType(), True),
                StructField("openDt", StringType(), True),
                StructField("active_from", StringType(), True),
                StructField("active", BooleanType(), True),
                StructField("srcName", StringType(), True),
                StructField("srcVer", StringType(), True),
                StructField("tz", StringType(), True),
                StructField("workstations", ArrayType(StructType([
                    StructField("wsNum", LongType(), True),
                    StructField("wsName", StringType(), True),
                ])), True),
            ])), True),
        ])

    def _get_menu_item_definitions_schema(self) -> StructType:
        """Schema for menu item definitions."""
        return StructType([
            StructField("locRef", StringType(), False),
            StructField("miNum", LongType(), False),
            StructField("miName", StringType(), True),
            StructField("miClass", LongType(), True),
            StructField("miPrice", DoubleType(), True),
        ])

    def _get_menu_item_dimensions_schema(self) -> StructType:
        """Schema for menu item dimensions."""
        return StructType([
            StructField("locRef", StringType(), False),
            StructField("miNum", LongType(), False),
            StructField("miName", StringType(), True),
            StructField("miClass", LongType(), True),
            StructField("price", DoubleType(), True),
        ])

    def _get_non_sales_item_definitions_schema(self) -> StructType:
        """Schema for non-sales item definitions."""
        return StructType([
            StructField("locRef", StringType(), False),
            StructField("itemNum", LongType(), False),
            StructField("itemName", StringType(), True),
            StructField("itemType", StringType(), True),
        ])

    def _get_order_type_dimensions_schema(self) -> StructType:
        """Schema for order type dimensions."""
        return StructType([
            StructField("locRef", StringType(), False),
            StructField("ordTypeNum", LongType(), False),
            StructField("ordTypeName", StringType(), True),
        ])

    def _get_revenue_center_dimensions_schema(self) -> StructType:
        """Schema for revenue center dimensions."""
        return StructType([
            StructField("locRef", StringType(), False),
            StructField("rvcNum", LongType(), False),
            StructField("rvcName", StringType(), True),
        ])

    def _get_service_charge_dimensions_schema(self) -> StructType:
        """Schema for service charge dimensions."""
        return StructType([
            StructField("locRef", StringType(), False),
            StructField("svcNum", LongType(), False),
            StructField("svcName", StringType(), True),
            StructField("svcRate", DoubleType(), True),
        ])

    def _get_tax_dimensions_schema(self) -> StructType:
        """Schema for tax dimensions."""
        return StructType([
            StructField("locRef", StringType(), False),
            StructField("taxNum", LongType(), False),
            StructField("taxName", StringType(), True),
            StructField("taxRate", DoubleType(), True),
        ])

    def _get_tender_media_dimensions_schema(self) -> StructType:
        """Schema for tender media dimensions."""
        return StructType([
            StructField("locRef", StringType(), False),
            StructField("tndNum", LongType(), False),
            StructField("tndName", StringType(), True),
            StructField("tndType", StringType(), True),
        ])

    def _get_time_card_code_dimensions_schema(self) -> StructType:
        """Schema for time card code dimensions."""
        return StructType([
            StructField("locRef", StringType(), False),
            StructField("timeCardCode", LongType(), False),
            StructField("timeCardName", StringType(), True),
        ])

    def _get_time_period_dimensions_schema(self) -> StructType:
        """Schema for time period dimensions."""
        return StructType([
            StructField("locRef", StringType(), False),
            StructField("periodNum", LongType(), False),
            StructField("periodName", StringType(), True),
            StructField("startTime", StringType(), True),
            StructField("endTime", StringType(), True),
        ])

    # ============================================================
    # PAYMENT DIMENSION SCHEMAS
    # ============================================================

    def _get_payment_gateway_dimensions_schema(self) -> StructType:
        """Schema for payment gateway dimensions."""
        return StructType([
            StructField("locRef", StringType(), False),
            StructField("gatewayId", StringType(), False),
            StructField("gatewayName", StringType(), True),
            StructField("gatewayType", StringType(), True),
        ])

    def _get_spi_payment_application_dimensions_schema(self) -> StructType:
        """Schema for SPI payment application dimensions."""
        return StructType([
            StructField("locRef", StringType(), False),
            StructField("appId", StringType(), False),
            StructField("appName", StringType(), True),
            StructField("appVersion", StringType(), True),
        ])

    # ============================================================
    # PAYMENT TRANSACTION SCHEMAS
    # ============================================================

    def _get_payment_details_schema(self) -> StructType:
        """Schema for payment details."""
        return StructType([
            StructField("locRef", StringType(), False),
            StructField("busDt", StringType(), False),
            StructField("paymentId", StringType(), False),
            StructField("chkNum", LongType(), True),
            StructField("paymentAmt", DoubleType(), True),
            StructField("paymentType", StringType(), True),
            StructField("paymentDtTm", StringType(), True),
        ])

    def _get_payment_status_changes_schema(self) -> StructType:
        """Schema for payment status changes."""
        return StructType([
            StructField("locRef", StringType(), False),
            StructField("busDt", StringType(), False),
            StructField("paymentId", StringType(), False),
            StructField("statusChangeDtTm", StringType(), True),
            StructField("oldStatus", StringType(), True),
            StructField("newStatus", StringType(), True),
        ])

    def _get_payment_tokens_schema(self) -> StructType:
        """Schema for payment tokens."""
        return StructType([
            StructField("locRef", StringType(), False),
            StructField("busDt", StringType(), False),
            StructField("tokenId", StringType(), False),
            StructField("tokenType", StringType(), True),
            StructField("last4", StringType(), True),
        ])

    def _get_refund_details_schema(self) -> StructType:
        """Schema for refund details."""
        return StructType([
            StructField("locRef", StringType(), False),
            StructField("busDt", StringType(), False),
            StructField("refundId", StringType(), False),
            StructField("originalPaymentId", StringType(), True),
            StructField("refundAmt", DoubleType(), True),
            StructField("refundDtTm", StringType(), True),
        ])

    def _get_tip_adjustment_details_schema(self) -> StructType:
        """Schema for tip adjustment details."""
        return StructType([
            StructField("locRef", StringType(), False),
            StructField("busDt", StringType(), False),
            StructField("adjustmentId", StringType(), False),
            StructField("chkNum", LongType(), True),
            StructField("tipAmt", DoubleType(), True),
            StructField("adjustmentDtTm", StringType(), True),
        ])

    # ============================================================
    # TRANSACTION SCHEMAS
    # ============================================================

    def _get_guest_checks_schema(self) -> StructType:
        """Schema for guest checks - nested structure with guestChecks array."""
        return StructType([
            StructField("curUTC", StringType(), True),
            StructField("locRef", StringType(), False),
            StructField("guestChecks", ArrayType(StructType([
                StructField("guestCheckId", LongType(), False),
                StructField("chkNum", LongType(), False),
                StructField("opnBusDt", StringType(), True),
                StructField("opnUTC", StringType(), True),
                StructField("opnLcl", StringType(), True),
                StructField("clsdBusDt", StringType(), True),
                StructField("clsdUTC", StringType(), True),
                StructField("clsdLcl", StringType(), True),
                StructField("lastTransUTC", StringType(), True),
                StructField("lastTransLcl", StringType(), True),
                StructField("lastUpdatedUTC", StringType(), True),
                StructField("lastUpdatedLcl", StringType(), True),
                StructField("clsdFlag", BooleanType(), True),
                StructField("subTtl", DoubleType(), True),
                StructField("nonTxblSlsTtl", DoubleType(), True),
                StructField("chkTtl", DoubleType(), True),
                StructField("dscTtl", DoubleType(), True),
                StructField("payTtl", DoubleType(), True),
                StructField("balDueTtl", DoubleType(), True),
                StructField("rvcNum", LongType(), True),
                StructField("otNum", LongType(), True),
                StructField("empNum", LongType(), True),
                StructField("taxes", ArrayType(StructType([
                    StructField("taxNum", LongType(), True),
                    StructField("txblSlsTtl", DoubleType(), True),
                    StructField("taxCollectedTtl", DoubleType(), True),
                ])), True),
                StructField("detailLines", ArrayType(StructType([
                    StructField("guestCheckLineItemId", LongType(), True),
                    StructField("dtlOtNum", LongType(), True),
                    StructField("lineNum", LongType(), True),
                    StructField("dtlId", LongType(), True),
                    StructField("parDtlId", LongType(), True),
                    StructField("detailUTC", StringType(), True),
                    StructField("detailLcl", StringType(), True),
                    StructField("lastUpdateUTC", StringType(), True),
                    StructField("lastUpdateLcl", StringType(), True),
                    StructField("busDt", StringType(), True),
                    StructField("wsNum", LongType(), True),
                    StructField("refInfo1", StringType(), True),
                    StructField("dspTtl", DoubleType(), True),
                    StructField("dspQty", DoubleType(), True),
                    StructField("aggTtl", DoubleType(), True),
                    StructField("aggQty", DoubleType(), True),
                ])), True),
            ])), True),
        ])

    def _get_guest_check_line_item_ext_details_schema(self) -> StructType:
        """Schema for guest check line item extensibility details - nested structure."""
        return StructType([
            StructField("locRef", StringType(), False),
            StructField("busDt", StringType(), False),
            StructField("revenueCenters", ArrayType(StructType([
                StructField("rvcNum", LongType(), True),
                StructField("extDetails", ArrayType(StructType([
                    StructField("guestCheckLineItemId", LongType(), True),
                    StructField("chkNum", LongType(), True),
                    StructField("dtlSeq", LongType(), True),
                    StructField("extKey", StringType(), True),
                    StructField("extValue", StringType(), True),
                ])), True),
            ])), True),
        ])

    def _get_guest_check_line_items_schema(self) -> StructType:
        """Schema for guest check line items."""
        return StructType([
            StructField("locRef", StringType(), False),
            StructField("busDt", StringType(), False),
            StructField("chkNum", LongType(), False),
            StructField("dtlSeq", LongType(), False),
            StructField("miNum", LongType(), True),
            StructField("miName", StringType(), True),
            StructField("qty", LongType(), True),
            StructField("price", DoubleType(), True),
            StructField("amount", DoubleType(), True),
        ])

    def _get_non_sales_transactions_schema(self) -> StructType:
        """Schema for non-sales transactions - nested structure."""
        return StructType([
            StructField("curUTC", StringType(), True),
            StructField("locRef", StringType(), False),
            StructField("busDt", StringType(), False),
            StructField("nonSalesTransactions", ArrayType(StructType([
                StructField("transUTC", StringType(), True),
                StructField("transLcl", StringType(), True),
                StructField("transType", LongType(), True),
                StructField("empNum", LongType(), True),
                StructField("wsNum", LongType(), True),
                StructField("value", DoubleType(), True),
                StructField("rvcNum", LongType(), True),
            ])), True),
        ])

    def _get_pos_journal_log_details_schema(self) -> StructType:
        """Schema for POS journal log details - nested structure."""
        return StructType([
            StructField("locRef", StringType(), False),
            StructField("busDt", StringType(), False),
            StructField("revenueCenters", ArrayType(StructType([
                StructField("rvcNum", LongType(), False),
                StructField("logDetails", ArrayType(StructType([
                    StructField("guestCheckId", LongType(), True),
                    StructField("type", LongType(), True),
                    StructField("transId", LongType(), True),
                    StructField("transUTC", StringType(), True),
                    StructField("transLcl", StringType(), True),
                    StructField("journalTxt", StringType(), True),
                ])), True),
            ])), True),
        ])

    def _get_pos_waste_details_schema(self) -> StructType:
        """Schema for POS waste details - nested structure."""
        return StructType([
            StructField("locRef", StringType(), False),
            StructField("busDt", StringType(), False),
            StructField("revenueCenters", ArrayType(StructType([
                StructField("rvcNum", LongType(), True),
                StructField("wasteDetails", ArrayType(StructType([
                    StructField("miNum", LongType(), True),
                    StructField("wasteQty", LongType(), True),
                    StructField("wasteUTC", StringType(), True),
                    StructField("wasteLcl", StringType(), True),
                    StructField("empNum", LongType(), True),
                    StructField("wsNum", LongType(), True),
                ])), True),
            ])), True),
        ])

    def _get_spi_payment_details_schema(self) -> StructType:
        """Schema for SPI payment details."""
        return StructType([
            StructField("locRef", StringType(), False),
            StructField("busDt", StringType(), False),
            StructField("chkNum", LongType(), True),
            StructField("tndNum", LongType(), True),
            StructField("tndTtl", DoubleType(), True),
            StructField("cardType", StringType(), True),
            StructField("last4", StringType(), True),
            StructField("authCode", StringType(), True),
        ])

    def read_table_metadata(
        self, table_name: str, table_options: Dict[str, str]
    ) -> dict:
        """Return metadata for all 67 Oracle Simphony BI API tables."""
        metadata_map = {
            # ============================================================
            # DAILY AGGREGATIONS (12 tables)
            # All have primary keys: locRef + busDt
            # ============================================================
            "combo_item_daily_totals": {
                "primary_keys": ["locRef", "busDt"],
                "ingestion_type": "snapshot",
            },
            "control_daily_totals": {
                "primary_keys": ["locRef", "busDt"],
                "ingestion_type": "snapshot",
            },
            "discount_daily_totals": {
                "primary_keys": ["locRef", "busDt"],
                "ingestion_type": "snapshot",
            },
            "employee_daily_totals": {
                "primary_keys": ["locRef", "busDt"],
                "ingestion_type": "snapshot",
            },
            "job_code_daily_totals": {
                "primary_keys": ["locRef", "busDt"],
                "ingestion_type": "snapshot",
            },
            "menu_item_daily_totals": {
                "primary_keys": ["locRef", "busDt"],
                "ingestion_type": "snapshot",
            },
            "operations_daily_totals": {
                "primary_keys": ["locRef", "busDt"],
                "ingestion_type": "snapshot",
            },
            "order_type_daily_totals": {
                "primary_keys": ["locRef", "busDt"],
                "ingestion_type": "snapshot",
            },
            "revenue_center_daily_totals": {
                "primary_keys": ["locRef", "busDt"],
                "ingestion_type": "snapshot",
            },
            "service_charge_daily_totals": {
                "primary_keys": ["locRef", "busDt"],
                "ingestion_type": "snapshot",
            },
            "tax_daily_totals": {
                "primary_keys": ["locRef", "busDt"],
                "ingestion_type": "snapshot",
            },
            "tender_media_daily_totals": {
                "primary_keys": ["locRef", "busDt"],
                "ingestion_type": "snapshot",
            },

            # ============================================================
            # QUARTER-HOUR AGGREGATIONS (8 tables)
            # All have primary keys: locRef + busDt
            # ============================================================
            "combo_item_quarter_hour_totals": {
                "primary_keys": ["locRef", "busDt"],
                "ingestion_type": "snapshot",
            },
            "discount_quarter_hour_totals": {
                "primary_keys": ["locRef", "busDt"],
                "ingestion_type": "snapshot",
            },
            "job_code_quarter_hour_totals": {
                "primary_keys": ["locRef", "busDt"],
                "ingestion_type": "snapshot",
            },
            "menu_item_quarter_hour_totals": {
                "primary_keys": ["locRef", "busDt"],
                "ingestion_type": "snapshot",
            },
            "operations_quarter_hour_totals": {
                "primary_keys": ["locRef", "busDt"],
                "ingestion_type": "snapshot",
            },
            "order_type_quarter_hour_totals": {
                "primary_keys": ["locRef", "busDt"],
                "ingestion_type": "snapshot",
            },
            "service_charge_quarter_hour_totals": {
                "primary_keys": ["locRef", "busDt"],
                "ingestion_type": "snapshot",
            },
            "tax_quarter_hour_totals": {
                "primary_keys": ["locRef", "busDt"],
                "ingestion_type": "snapshot",
            },

            # ============================================================
            # CASH MANAGEMENT (1 table)
            # ============================================================
            "cash_management_details": {
                "primary_keys": ["locRef", "busDt", "transSeq"],
                "ingestion_type": "snapshot",
            },

            # ============================================================
            # FISCAL TRANSACTIONS (3 tables)
            # ============================================================
            "fiscal_invoice_control_data": {
                "primary_keys": ["locRef", "busDt", "invoiceNum"],
                "ingestion_type": "snapshot",
            },
            "fiscal_invoice_data": {
                "primary_keys": ["locRef", "busDt", "invoiceNum"],
                "ingestion_type": "snapshot",
            },
            "fiscal_total_data": {
                "primary_keys": ["locRef", "busDt", "fiscalTotalType"],
                "ingestion_type": "snapshot",
            },

            # ============================================================
            # KITCHEN PERFORMANCE & LABOR (2 tables)
            # ============================================================
            "kds_details": {
                "primary_keys": ["locRef", "busDt", "chkNum"],
                "ingestion_type": "snapshot",
            },
            "time_card_details": {
                "primary_keys": ["locRef", "busDt", "empNum", "clockInDtTm"],
                "ingestion_type": "snapshot",
            },

            # ============================================================
            # POS DIMENSIONS (15 tables)
            # Most have primary keys: locRef + dimension ID
            # location_dimensions is special: only locRef
            # ============================================================
            "combo_meal_definitions": {
                "primary_keys": ["locRef", "comboNum"],
                "ingestion_type": "snapshot",
            },
            "discount_dimensions": {
                "primary_keys": ["locRef", "dscNum"],
                "ingestion_type": "snapshot",
            },
            "employee_dimensions": {
                "primary_keys": ["locRef", "empNum"],
                "ingestion_type": "snapshot",
            },
            "job_code_dimensions": {
                "primary_keys": ["locRef", "jobCode"],
                "ingestion_type": "snapshot",
            },
            "location_dimensions": {
                "primary_keys": [],  # No root-level unique identifier
                "ingestion_type": "snapshot",
            },
            "menu_item_definitions": {
                "primary_keys": ["locRef", "miNum"],
                "ingestion_type": "snapshot",
            },
            "menu_item_dimensions": {
                "primary_keys": ["locRef", "miNum"],
                "ingestion_type": "snapshot",
            },
            "non_sales_item_definitions": {
                "primary_keys": ["locRef", "itemNum"],
                "ingestion_type": "snapshot",
            },
            "order_type_dimensions": {
                "primary_keys": ["locRef", "ordTypeNum"],
                "ingestion_type": "snapshot",
            },
            "revenue_center_dimensions": {
                "primary_keys": ["locRef", "rvcNum"],
                "ingestion_type": "snapshot",
            },
            "service_charge_dimensions": {
                "primary_keys": ["locRef", "svcNum"],
                "ingestion_type": "snapshot",
            },
            "tax_dimensions": {
                "primary_keys": ["locRef", "taxNum"],
                "ingestion_type": "snapshot",
            },
            "tender_media_dimensions": {
                "primary_keys": ["locRef", "tndNum"],
                "ingestion_type": "snapshot",
            },
            "time_card_code_dimensions": {
                "primary_keys": ["locRef", "timeCardCode"],
                "ingestion_type": "snapshot",
            },
            "time_period_dimensions": {
                "primary_keys": ["locRef", "periodNum"],
                "ingestion_type": "snapshot",
            },

            # ============================================================
            # PAYMENT DIMENSIONS (2 tables)
            # ============================================================
            "payment_gateway_dimensions": {
                "primary_keys": ["locRef", "gatewayId"],
                "ingestion_type": "snapshot",
            },
            "spi_payment_application_dimensions": {
                "primary_keys": ["locRef", "appId"],
                "ingestion_type": "snapshot",
            },

            # ============================================================
            # PAYMENT TRANSACTIONS (5 tables)
            # ============================================================
            "payment_details": {
                "primary_keys": ["locRef", "busDt", "paymentId"],
                "ingestion_type": "snapshot",
            },
            "payment_status_changes": {
                "primary_keys": ["locRef", "busDt", "paymentId", "statusChangeDtTm"],
                "ingestion_type": "snapshot",
            },
            "payment_tokens": {
                "primary_keys": ["locRef", "busDt", "tokenId"],
                "ingestion_type": "snapshot",
            },
            "refund_details": {
                "primary_keys": ["locRef", "busDt", "refundId"],
                "ingestion_type": "snapshot",
            },
            "tip_adjustment_details": {
                "primary_keys": ["locRef", "busDt", "adjustmentId"],
                "ingestion_type": "snapshot",
            },

            # ============================================================
            # TRANSACTIONS (7 tables)
            # ============================================================
            "guest_checks": {
                "primary_keys": ["locRef"],
                "ingestion_type": "snapshot",
            },
            "guest_check_line_item_ext_details": {
                "primary_keys": ["locRef", "busDt"],
                "ingestion_type": "snapshot",
            },
            "guest_check_line_items": {
                "primary_keys": ["locRef", "busDt", "chkNum", "dtlSeq"],
                "ingestion_type": "snapshot",
            },
            "non_sales_transactions": {
                "primary_keys": ["locRef", "busDt"],
                "ingestion_type": "snapshot",
            },
            "pos_journal_log_details": {
                "primary_keys": ["locRef", "busDt"],
                "ingestion_type": "snapshot",
            },
            "pos_waste_details": {
                "primary_keys": ["locRef", "busDt"],
                "ingestion_type": "snapshot",
            },
            "spi_payment_details": {
                "primary_keys": ["locRef", "busDt", "chkNum"],
                "ingestion_type": "snapshot",
            },
        }

        if table_name not in metadata_map:
            raise ValueError(f"Unsupported table: {table_name}")

        return metadata_map[table_name]

    def read_table(
        self, table_name: str, start_offset: dict, table_options: Dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read data from specified table across all locations."""
        config = self._endpoint_config.get(table_name)
        if not config:
            raise ValueError(f"Unsupported table: {table_name}")

        # Special case: location_dimensions returns all locations already
        if table_name == "location_dimensions":
            return self._read_location_dimensions_table(table_options)

        # For all other tables: iterate through locations (Mixpanel pattern)
        return self._read_table_multi_location(table_name, config, start_offset, table_options)

    def _read_location_dimensions_table(
        self, table_options: Dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Special handler for location_dimensions table."""
        request_body = {
            "applicationName": table_options.get("applicationName", "LakeflowConnect")
        }
        endpoint = "getLocationDimensions"
        url = f"{self.api_base}/{endpoint}"

        try:
            response = self._session.post(url, json=request_body, timeout=self.timeout)
            self._handle_response_errors(response)
            data = response.json()

            # Enrich each location with active_from field (set to openDt)
            if "locations" in data and isinstance(data["locations"], list):
                for location in data["locations"]:
                    # Set active_from to openDt (opening date is the start date)
                    location["active_from"] = location.get("openDt")

            return iter([data]), {}
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"Failed to read location_dimensions: {str(e)}")

    def _read_table_multi_location(
        self, table_name: str, config: dict, start_offset: dict, table_options: Dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """
        Read table data for ONE location per batch (enables Spark parallelization).

        Spark will call this method multiple times in parallel, each processing a different location.
        Offset tracks progress through locations, allowing Spark to parallelize and checkpoint.

        Offset structure:
        {
            "bus_dt": "2024-01-31",
            "current_location_index": 0,  # Which location to process
            "completed": False  # True when all locations processed
        }
        """
        # Fetch locations (cached after first call)
        locations = self._fetch_and_cache_locations()

        # Determine which location to process (from offset)
        current_location_index = start_offset.get("current_location_index", 0) if start_offset else 0

        # Check if we've already completed all locations
        if start_offset and start_offset.get("completed", False):
            print(f"[INFO] All {len(locations)} locations already processed for {table_name}")
            return iter([]), start_offset

        # Check if we're beyond the location list
        if current_location_index >= len(locations):
            print(f"[INFO] Completed all {len(locations)} locations for {table_name}")
            # Use business date from offset if available, otherwise current date
            bus_dt = start_offset.get("bus_dt") if start_offset else datetime.now().strftime("%Y-%m-%d")
            next_offset = {
                "bus_dt": bus_dt,
                "current_location_index": current_location_index,
                "completed": True
            }
            return iter([]), next_offset

        # Process ONLY the current location
        location = locations[current_location_index]
        loc_ref = location["locRef"]
        loc_name = location.get("name", "Unknown")

        print(f"[INFO] Reading {table_name} for location {loc_ref} ({loc_name}) - {current_location_index+1}/{len(locations)}")

        # Determine business date for THIS location (fetches latest if not specified)
        bus_dt = self._resolve_business_date(start_offset, table_options, loc_ref)

        # Build request body for this location
        request_body = {}
        if config.get("requires_busdt", True):
            request_body["busDt"] = bus_dt
        if config.get("requires_locref", True):
            request_body["locRef"] = loc_ref

        # Add optional parameters
        if config.get("requires_application_name", False):
            request_body["applicationName"] = table_options.get("applicationName", "LakeflowConnect")
        if "applicationName" in table_options and not config.get("requires_application_name", False) and not config.get("blocks_application_name", False):
            request_body["applicationName"] = table_options["applicationName"]
        if "searchCriteria" in table_options:
            request_body["searchCriteria"] = table_options["searchCriteria"]
        if "include" in table_options:
            request_body["include"] = table_options["include"]

        # Make API request for THIS location
        endpoint = config["endpoint"]
        url = f"{self.api_base}/{endpoint}"

        try:
            response = self._session.post(url, json=request_body, timeout=self.timeout)
            self._handle_response_errors(response)
            data = response.json()

            # Parse response
            records = self._parse_response(data, config)

            print(f"[INFO] Fetched {len(records)} records from location {loc_ref}")

        except requests.exceptions.RequestException as e:
            # Log error and return empty for this location (Spark will retry)
            print(f"[ERROR] Failed to read location {loc_ref}: {str(e)}")
            raise RuntimeError(f"Failed to fetch data for location {loc_ref}: {str(e)}")

        # Calculate next offset (move to next location)
        next_location_index = current_location_index + 1

        if next_location_index >= len(locations):
            # This was the last location
            next_offset = {
                "bus_dt": bus_dt,
                "current_location_index": next_location_index,
                "completed": True
            }
            print(f"[INFO] Location {loc_ref} complete - all {len(locations)} locations processed")
        else:
            # More locations to process
            next_offset = {
                "bus_dt": bus_dt,
                "current_location_index": next_location_index,
                "completed": False
            }
            print(f"[INFO] Location {loc_ref} complete - {len(locations) - next_location_index} locations remaining")

        return iter(records), next_offset

    def _resolve_business_date(
        self, start_offset: dict, table_options: Dict[str, str], loc_ref: str = None
    ) -> str:
        """
        Resolve business date with the following priority:
        1. start_offset["bus_dt"] - Continuation from previous read
        2. table_options["bus_dt"] - Explicit user override
        3. API latest business date for location - Fetched from getLatestBusDt
        4. Current date - Fallback only

        Args:
            start_offset: Offset from previous read
            table_options: User-provided table options
            loc_ref: Location reference (required for priority 3)

        Returns:
            Business date string in "YYYY-MM-DD" format
        """
        # Priority 1: Continuation from previous read
        if start_offset and "bus_dt" in start_offset:
            return start_offset["bus_dt"]

        # Priority 2: Explicit user override in table options
        if "bus_dt" in table_options:
            return table_options["bus_dt"]

        # Priority 3: Fetch latest business date from API for this location
        if loc_ref:
            try:
                return self._fetch_latest_business_date_for_location(loc_ref)
            except Exception as e:
                print(f"[WARNING] Failed to fetch latest business date for location {loc_ref}: {e}")
                print(f"[WARNING] Falling back to current date")

        # Priority 4: Fallback to current date (should rarely happen)
        return datetime.now().strftime("%Y-%m-%d")

    def _parse_response(self, data: dict, config: dict) -> List[dict]:
        """Parse API response based on endpoint configuration."""
        try:
            parse_type = config.get("parse_type", "flat")

            if parse_type == "flat":
                # For flat responses, return as-is or extract records array
                if isinstance(data, list):
                    return data
                elif isinstance(data, dict):
                    # Check for common response wrappers
                    if "records" in data:
                        return data["records"]
                    elif "data" in data:
                        return data["data"]
                    else:
                        # Return single record wrapped in list
                        return [data]
                return []

            elif parse_type == "keep_nested":
                # Keep nested structure as-is - return single record
                return [data]

            else:
                raise ValueError(f"Unknown parse_type: {parse_type}")

        except KeyError as e:
            raise ValueError(
                f"Unexpected API response structure: missing key {e}"
            )
        except Exception as e:
            raise RuntimeError(f"Failed to parse API response: {str(e)}")
