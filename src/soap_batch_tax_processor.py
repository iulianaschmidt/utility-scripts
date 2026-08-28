"""
Batch SOAP Tax Calculation Processor
Processes production data (CSV/Excel) against a SOAP tax calculation endpoint.
Supports parallel processing, error handling, and detailed result logging.
"""

import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
import xml.etree.ElementTree as ET
from xml.dom import minidom
import argparse
import logging
import os
import subprocess
import sys
from datetime import datetime
from decimal import Decimal, InvalidOperation
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import json
import time
import threading
import traceback
from collections import Counter


SOAP_NAMESPACES = {
    'soap': 'http://schemas.xmlsoap.org/soap/envelope/',
    'tax': 'http://www.sabrix.com/services/taxcalculationservice/2011-09-01'
}

VENV_RELAUNCH_FLAG = 'SOAP_BATCH_VENV_BOOTSTRAPPED'


def ensure_project_venv(project_root):
    """Relaunch with .venv Python when available and not already active."""
    if os.environ.get(VENV_RELAUNCH_FLAG) == '1':
        return

    venv_python = project_root / '.venv' / 'Scripts' / 'python.exe'
    if not venv_python.exists():
        print(
            ".venv not found. Running with current Python interpreter:\n"
            f"  {sys.executable}\n"
            "Create it once with: py -3.8 -m venv .venv"
        )
        return

    try:
        if Path(sys.executable).resolve() == venv_python.resolve():
            return
    except OSError:
        pass

    print(f"Restarting with project virtual environment: {venv_python}")
    env = os.environ.copy()
    env[VENV_RELAUNCH_FLAG] = '1'
    env.setdefault('PYTHONUNBUFFERED', '1')
    completed = subprocess.run([str(venv_python)] + sys.argv, env=env)
    raise SystemExit(completed.returncode)

# OUTDATA export columns aligned with the formatted Sabrix CSV style.
OUTDATA_FORMATTED_COLUMNS = [
    'source_file_name',
    'request_document_id',
    'calling_system_number',
    'host_system',
    'company_role',
    'currency_code',
    'invoice_header_date',
    'invoice_tax_determination_date',
    'invoice_transaction_type',
    'line_id',
    'commodity_code',
    'delivery_terms',
    'is_business_supply',
    'is_credit',
    'line_invoice_date',
    'line_number',
    'order_acceptance_country',
    'order_acceptance_state',
    'order_acceptance_postcode',
    'order_acceptance_geocode',
    'order_origin_country',
    'order_origin_state',
    'order_origin_postcode',
    'order_origin_geocode',
    'ship_from_country',
    'ship_from_state',
    'ship_from_postcode',
    'ship_from_geocode',
    'ship_from_location_tax_category',
    'invoice_number',
    'original_invoice_number',
    'invoice_date',
    'external_company_id',
    'customer_number',
    'gross_amount',
    'ship_to_country',
    'ship_to_state',
    'ship_to_postcode',
    'ship_to_geocode',
    'quantity',
    'unit_of_measure',
    'supplementary_unit',
    'part_number',
    'country_of_origin',
    'point_of_title_transfer',
    'title_transfer_location',
    'product_code',
    'regime',
    'line_tax_determination_date',
    'line_transaction_type',
    'inclusive_tax_indicators',
    'exempt_amount_country',
    'exempt_amount_province',
    'exempt_amount_state',
    'exempt_amount_county',
    'exempt_amount_city',
    'exempt_amount_district',
    'exempt_amount_postcode',
    'exempt_amount_geocode',
    'source_system',
    'attribute_1',
    'attribute_24',
    'attribute_48',
    'attribute_2',
    'attribute_5',
    'attribute_6',
    'attribute_9',
    'attribute_10',
    'attribute_14',
    'attribute_18',
    'attribute_19',
    'attribute_20',
    'attribute_22',
    'attribute_26',
    'attribute_32',
    'attribute_37',
    'attribute_39',
    'tax_summary_taxable_basis',
    'tax_summary_non_taxable_basis',
    'tax_summary_exempt_amount',
    'tax_summary_tax_rate',
    'tax_summary_effective_tax_rate',
    'tax_summary_advisory',
    'total_tax_amount',
    'outdata_is_success',
    'outdata_error_code',
    'outdata_error_description',
]

# ============================================================================
# CONFIGURATION SECTION - CUSTOMIZE THIS FOR YOUR NEEDS
# ============================================================================

# SOAP Endpoint Details
#BFQ/BFT
SOAP_ENDPOINT = "https://onesource-idt-det-uat-ws.hostedtax.thomsonreuters.com/sabrix/services/taxcalculationservice/2011-09-01/taxcalculationservice"
SOAP_ACTION = "http://www.sabrix.com/services/taxcalculationservice/2011-09-01"


#BFP
#SOAP_ENDPOINT = "https://onesource-idt-det-amer-ws.hostedtax.thomsonreuters.com/sabrix/services/taxcalculationservice/2011-09-01/taxcalculationservice"
#SOAP_ACTION = "http://www.sabrix.com/services/taxcalculationservice/2011-09-01"


# Authentication Credentials
#bft/bfq CREDENTIALS
SOAP_USERNAME = "^bridgestoneuat_ws"
SOAP_PASSWORD = "6LktRIzIhjAVlSlC"

#BFP CREDENTIALS

#SOAP_USERNAME="^bridgestoneprod_ws"
##SOAP_PASSWORD="Th2p@D9a7IPtR"


# Input/Output Files
INPUT_FILE = "C:\\Users\\schmidtjulie\\Documents\\PYScripts\\PythonScripts\\input\\input_0714.csv"  # or .xlsx
OUTPUT_FILE = "C:\\Users\\schmidtjulie\\Documents\\PYScripts\\PythonScripts\\output\\uat0827_Data_response_results.csv"
OUTDATA_OUTPUT_FILE = "C:\\Users\\schmidtjulie\\Documents\\PYScripts\\PythonScripts\\output\\uat0827Data_soap_response_outdata072326_2.csv"
LOG_FILE = "C:\\Users\\schmidtjulie\\Documents\\PYScripts\\PythonScripts\\output\\PROD0724Data_soap_batch_processor.log"
LOAD_TEST_RESULTS_FILE = "C:\\Users\\schmidtjulie\\Documents\\PYScripts\\PythonScripts\\output\\PROD0724Data_soap_load_test_results.csv"
LOAD_TEST_SUMMARY_FILE = "C:\\Users\\schmidtjulie\\Documents\\PYScripts\\PythonScripts\\output\\PROD0724Data_soap_load_test_summary.json"
LOAD_TEST_FAILURES_FILE = "C:\\Users\\schmidtjulie\\Documents\\PYScripts\\PythonScripts\\output\\PROD0724Data_soap_load_test_failures.csv"

# Processing Configuration
MAX_WORKERS = 5  # Number of parallel requests (1-10 recommended)
REQUEST_TIMEOUT = 60  # Seconds
REQUEST_TIMEOUT = 60  # Seconds
RETRY_FAILED_ROWS = False
MAX_RETRIES = 1

# ============================================================================
# SOURCE SYSTEM MAPPING CONFIGURATION
# ============================================================================
# The script automatically detects SAP vs non-SAP sources based on the 'source_system' column
# SAP sources: 'SAP', 'S', 'YES', 'TRUE', '1' → Full SAP attribute set
# Non-SAP sources: 'NON_SAP', 'OTHER', or empty → Simplified attributes + CUSTOMER_NAME/PRODUCT_CODE
# ============================================================================

# Column Mapping - Map your Alteryx columns to SOAP request fields
# UPDATE THIS based on your Alteryx output columns
COLUMN_MAPPING = {
    # Invoice-level fields
    'invoice_number': 'INVOICE_NUMBER',
    'invoice_date': 'INVOICE_DATE',  # Format: YYYYMMDD
    'external_company_id': 'EXTERNAL_COMPANY_ID',
    'currency_code': 'CURRENCY_CODE',
    'customer_number': 'CUSTOMER_NUMBER',
    'part_number': 'PART_NUMBER',
    
    # Location fields (Ship To)
    'ship_to_country': 'SHIP_TO_COUNTRY',
    'ship_to_state': 'SHIP_TO_STATE',
    'ship_to_city': 'SHIP_TO_CITY',
    'ship_to_postcode': 'SHIP_TO_POSTCODE',
    'ship_to_geocode': 'SHIP_TO_GEOCODE',
    
    # Line-level fields
    'gross_amount': 'GROSS_AMOUNT',
    'unit_of_measure': 'UNIT_OF_MEASURE',
    'quantity': 'QUANTITY',
    'country_of_origin': 'COUNTRY_OF_ORIGIN',
    
    # Source system indicator
    'source_system': 'SOURCE_SYSTEM',  # 'SAP' or 'NON_SAP'
    
    # Custom attributes (if needed)
    'attribute_1': 'ATTRIBUTE1',
    'attribute_24': 'ATTRIBUTE24',
    'attribute_48': 'ATTRIBUTE48',
}

# SAP-specific column mapping (additional fields for SAP sources)
SAP_COLUMN_MAPPING = {
    'attribute_2': 'ATTRIBUTE2',
    'attribute_5': 'ATTRIBUTE5',
    'attribute_6': 'ATTRIBUTE6',
    'attribute_9': 'ATTRIBUTE9',
    'attribute_10': 'ATTRIBUTE10',
    'attribute_14': 'ATTRIBUTE14',
    'attribute_18': 'ATTRIBUTE18',
    'attribute_19': 'ATTRIBUTE19',
    'attribute_20': 'ATTRIBUTE20',
    'attribute_22': 'ATTRIBUTE22',
    'attribute_26': 'ATTRIBUTE26',
    'attribute_32': 'ATTRIBUTE32',
    'attribute_37': 'ATTRIBUTE37',
    'attribute_39': 'ATTRIBUTE39',
}

# Non-SAP column mapping (different or fewer attributes)
NON_SAP_COLUMN_MAPPING = {
    'customer_name': 'CUSTOMER_NAME',
    'product_code': 'PRODUCT_CODE',
    'commodity_code': 'COMMODITY_CODE',
}

# USER_ELEMENT mapping aliases used in SOAP request generation.
USER_ELEMENT_MAPPING = {
    'ATTRIBUTE1': ('ATTRIBUTE1', 'attribute_1', 'attribute1'),
    'ATTRIBUTE2': ('ATTRIBUTE2', 'attribute_2', 'attribute2'),
    'ATTRIBUTE5': ('ATTRIBUTE5', 'attribute_5', 'attribute5'),
    'ATTRIBUTE6': ('ATTRIBUTE6', 'attribute_6', 'attribute6'),
    'ATTRIBUTE9': ('ATTRIBUTE9', 'attribute_9', 'attribute9'),
    'ATTRIBUTE10': ('ATTRIBUTE10', 'attribute_10', 'attribute10'),
    'ATTRIBUTE14': ('ATTRIBUTE14', 'attribute_14', 'attribute14'),
    'ATTRIBUTE18': ('ATTRIBUTE18', 'attribute_18', 'attribute18'),
    'ATTRIBUTE19': ('ATTRIBUTE19', 'attribute_19', 'attribute19'),
    'ATTRIBUTE20': ('ATTRIBUTE20', 'attribute_20', 'attribute20'),
    'ATTRIBUTE22': ('ATTRIBUTE22', 'attribute_22', 'attribute22'),
    'ATTRIBUTE24': ('ATTRIBUTE24', 'attribute_24', 'attribute24'),
    'ATTRIBUTE26': ('ATTRIBUTE26', 'attribute_26', 'attribute26'),
    'ATTRIBUTE32': ('ATTRIBUTE32', 'attribute_32', 'attribute32'),
    'ATTRIBUTE37': ('ATTRIBUTE37', 'attribute_37', 'attribute37'),
    'ATTRIBUTE39': ('ATTRIBUTE39', 'attribute_39', 'attribute39'),
    'ATTRIBUTE48': ('ATTRIBUTE48', 'attribute_48', 'attribute48'),
}

# Fallback VALUE used when a mapped USER_ELEMENT source value is missing.
USER_ELEMENT_FALLBACK_VALUE = 'NA'

# Default values for fields that may not be in your data
DEFAULTS = {
    'CALLING_SYSTEM_NUMBER': '030',
    'HOST_SYSTEM': 'BFP',
    'COMPANY_ROLE': 'S',
    'IS_AUDITED': 'N',
    'IS_ROUNDING': 'Y',
    'TRANSACTION_TYPE': 'GS',
    'REGIME': '1',
    'IS_BUSINESS_SUPPLY': 'N',
    'IS_CREDIT': 'N',
    'CURRENCY_CODE': 'USD',
    'UNIT_OF_MEASURE': 'EA',
    'COUNTRY_OF_ORIGIN': 'US',
    'COMMODITY_CODE': '',
    'DELIVERY_TERMS': '',
    'LINE_ID': '1',
    'LINE_NUMBER': '1',
}

# ============================================================================
# SETUP LOGGING
# ============================================================================

def setup_logging(log_file):
    """Configure logging to file and console"""
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

logger = setup_logging(LOG_FILE)


def local_name(tag):
    """Return XML local tag name without namespace."""
    if '}' in tag:
        return tag.split('}', 1)[1]
    return tag


def find_first_by_local_name(elem, name):
    """Find the first element by local tag name, regardless of namespace."""
    target = name.upper()
    for node in elem.iter():
        if local_name(node.tag).upper() == target:
            return node
    return None


def find_all_by_local_name(elem, name):
    """Find all elements by local tag name, regardless of namespace."""
    target = name.upper()
    return [node for node in elem.iter() if local_name(node.tag).upper() == target]


def parse_response_xml(content_bytes):
    """Parse XML payload with a fallback that trims non-XML prefix noise."""
    try:
        return ET.fromstring(content_bytes)
    except ET.ParseError:
        text = content_bytes.decode('utf-8', errors='replace')
        start = text.find('<')
        if start >= 0:
            return ET.fromstring(text[start:])
        raise


def normalize_xml_text(text):
    """Normalize XML text node values for compact JSON output."""
    if text is None:
        return ''
    return ' '.join(text.split())


def xml_escape(value):
    """Escape XML-special characters in dynamic values."""
    if value is None:
        return ''
    text = str(value)
    return (
        text.replace('&', '&amp;')
        .replace('<', '&lt;')
        .replace('>', '&gt;')
        .replace('"', '&quot;')
        .replace("'", '&apos;')
    )


def normalize_request_document_id(value):
    """Convert request document id to a plain, human-readable number string."""
    if value is None:
        return ''

    text = str(value).strip()
    if not text or text.lower() == 'nan':
        return ''

    try:
        numeric = Decimal(text)
    except (InvalidOperation, ValueError):
        return text

    if numeric == numeric.to_integral_value():
        return str(numeric.quantize(Decimal('1')))

    plain = format(numeric.normalize(), 'f')
    return plain.rstrip('0').rstrip('.')


def trimmed_text(value, limit=2000):
    """Return compact text suitable for diagnostics columns."""
    if value is None:
        return ''
    text = str(value).strip()
    if len(text) <= limit:
        return text
    return text[: limit - 3] + '...'


def exception_details(exc):
    """Build exception class/message and a short traceback for CSV diagnostics."""
    return {
        'error_type': exc.__class__.__name__,
        'error_details': trimmed_text(''.join(traceback.format_exception(type(exc), exc, exc.__traceback__)), limit=4000)
    }


def percentile(values, p):
    """Simple percentile helper without external dependencies."""
    if not values:
        return 0.0
    ordered = sorted(float(v) for v in values)
    if len(ordered) == 1:
        return ordered[0]

    rank = (len(ordered) - 1) * (p / 100.0)
    low = int(rank)
    high = min(low + 1, len(ordered) - 1)
    weight = rank - low
    return ordered[low] * (1.0 - weight) + ordered[high] * weight


def element_to_data_excluding_message(elem):
    """Convert an XML element into nested dict/list while skipping all MESSAGE blocks."""
    tag_name = local_name(elem.tag).upper()
    if tag_name == 'MESSAGE':
        return None

    filtered_children = [
        child for child in list(elem)
        if local_name(child.tag).upper() != 'MESSAGE'
    ]

    node = {}

    # Preserve attributes (e.g., LINE ID="1").
    for attr_name, attr_value in elem.attrib.items():
        node[f'@{attr_name}'] = attr_value

    if not filtered_children:
        text_value = normalize_xml_text(elem.text)
        if node:
            if text_value:
                node['#text'] = text_value
            return node
        return text_value

    grouped = {}
    for child in filtered_children:
        child_name = local_name(child.tag)
        child_data = element_to_data_excluding_message(child)
        if child_data is None:
            continue
        grouped.setdefault(child_name, []).append(child_data)

    for child_name, values in grouped.items():
        node[child_name] = values[0] if len(values) == 1 else values

    text_value = normalize_xml_text(elem.text)
    if text_value and node:
        node['#text'] = text_value

    return node


def extract_outdata_mapping(root):
    """Extract and map OUTDATA from SOAP response, excluding every MESSAGE block."""
    outdata = find_first_by_local_name(root, 'OUTDATA')
    if outdata is None:
        return None

    return {'OUTDATA': element_to_data_excluding_message(outdata)}


def direct_child(elem, name):
    """Return direct child element by local name (namespace-agnostic)."""
    target = name.upper()
    for child in list(elem):
        if local_name(child.tag).upper() == target:
            return child
    return None


def direct_children(elem, name):
    """Return direct child elements by local name (namespace-agnostic)."""
    target = name.upper()
    return [child for child in list(elem) if local_name(child.tag).upper() == target]


def direct_text(elem, name, default=''):
    """Return normalized text for a direct child tag."""
    if elem is None:
        return default
    child = direct_child(elem, name)
    if child is None:
        return default
    value = normalize_xml_text(child.text)
    return value if value else default


def empty_outdata_formatted_row(row_data=None):
    """Build blank OUTDATA row with input identifiers populated where available."""
    row = {column: '' for column in OUTDATA_FORMATTED_COLUMNS}
    source_row = row_data or {}
    row['source_file_name'] = (
        source_row.get('source_file_name')
        or source_row.get('SOURCE_FILE_NAME')
        or ''
    )
    row['request_document_id'] = normalize_request_document_id(
        source_row.get('request_document_id') or source_row.get('REQUEST_DOCUMENT_ID')
    )
    return row


def extract_outdata_formatted_row(root, row_data):
    """Flatten OUTDATA into line-level columns similar to format_sabrix_logs_to_csv."""
    formatted = empty_outdata_formatted_row(row_data)

    outdata = find_first_by_local_name(root, 'OUTDATA')
    if outdata is None:
        return formatted

    outdata_status = direct_child(outdata, 'REQUEST_STATUS')
    if outdata_status is not None:
        formatted['outdata_is_success'] = direct_text(outdata_status, 'IS_SUCCESS')
        outdata_error = direct_child(outdata_status, 'ERROR')
        if outdata_error is not None:
            formatted['outdata_error_code'] = direct_text(outdata_error, 'CODE')
            formatted['outdata_error_description'] = direct_text(outdata_error, 'DESCRIPTION')

    invoice = direct_child(outdata, 'INVOICE')
    if invoice is None:
        return formatted

    formatted['calling_system_number'] = direct_text(invoice, 'CALLING_SYSTEM_NUMBER')
    formatted['host_system'] = direct_text(invoice, 'HOST_SYSTEM')
    formatted['company_role'] = direct_text(invoice, 'COMPANY_ROLE')
    formatted['currency_code'] = direct_text(invoice, 'CURRENCY_CODE')
    formatted['invoice_header_date'] = direct_text(invoice, 'INVOICE_DATE')
    formatted['invoice_tax_determination_date'] = direct_text(invoice, 'TAX_DETERMINATION_DATE')
    formatted['invoice_transaction_type'] = direct_text(invoice, 'TRANSACTION_TYPE')
    formatted['invoice_number'] = direct_text(invoice, 'INVOICE_NUMBER')
    formatted['original_invoice_number'] = direct_text(invoice, 'ORIGINAL_INVOICE_NUMBER')
    formatted['invoice_date'] = direct_text(invoice, 'INVOICE_DATE')
    formatted['external_company_id'] = direct_text(invoice, 'EXTERNAL_COMPANY_ID')
    formatted['total_tax_amount'] = direct_text(invoice, 'TOTAL_TAX_AMOUNT')

    invoice_status = direct_child(invoice, 'REQUEST_STATUS')
    if invoice_status is not None and not formatted['outdata_is_success']:
        formatted['outdata_is_success'] = direct_text(invoice_status, 'IS_SUCCESS')
    if invoice_status is not None and not formatted['outdata_error_code']:
        invoice_error = direct_child(invoice_status, 'ERROR')
        if invoice_error is not None:
            formatted['outdata_error_code'] = direct_text(invoice_error, 'CODE')
            formatted['outdata_error_description'] = direct_text(invoice_error, 'DESCRIPTION')

    line = direct_child(invoice, 'LINE')
    if line is None:
        lines = find_all_by_local_name(invoice, 'LINE')
        line = lines[0] if lines else None
    if line is None:
        return formatted

    formatted['line_id'] = line.attrib.get('ID', '')
    formatted['commodity_code'] = direct_text(line, 'COMMODITY_CODE')
    formatted['delivery_terms'] = direct_text(line, 'DELIVERY_TERMS')
    formatted['is_business_supply'] = direct_text(line, 'IS_BUSINESS_SUPPLY')
    formatted['is_credit'] = direct_text(line, 'IS_CREDIT')
    formatted['line_invoice_date'] = direct_text(line, 'INVOICE_DATE')
    formatted['line_number'] = direct_text(line, 'LINE_NUMBER')
    formatted['customer_number'] = direct_text(line, 'CUSTOMER_NUMBER')
    formatted['gross_amount'] = direct_text(line, 'GROSS_AMOUNT')
    formatted['part_number'] = direct_text(line, 'PART_NUMBER')
    formatted['country_of_origin'] = direct_text(line, 'COUNTRY_OF_ORIGIN')
    formatted['point_of_title_transfer'] = direct_text(line, 'POINT_OF_TITLE_TRANSFER')
    formatted['title_transfer_location'] = direct_text(line, 'TITLE_TRANSFER_LOCATION')
    formatted['product_code'] = direct_text(line, 'PRODUCT_CODE')
    formatted['regime'] = direct_text(line, 'REGIME')
    formatted['line_tax_determination_date'] = direct_text(line, 'TAX_DETERMINATION_DATE')
    formatted['line_transaction_type'] = direct_text(line, 'TRANSACTION_TYPE')
    formatted['inclusive_tax_indicators'] = direct_text(line, 'INCLUSIVE_TAX_INDICATORS')
    formatted['source_system'] = direct_text(line, 'SOURCE_SYSTEM')

    order_acceptance = direct_child(line, 'ORDER_ACCEPTANCE')
    formatted['order_acceptance_country'] = direct_text(order_acceptance, 'COUNTRY')
    formatted['order_acceptance_state'] = direct_text(order_acceptance, 'STATE')
    formatted['order_acceptance_postcode'] = direct_text(order_acceptance, 'POSTCODE')
    formatted['order_acceptance_geocode'] = direct_text(order_acceptance, 'GEOCODE')

    order_origin = direct_child(line, 'ORDER_ORIGIN')
    formatted['order_origin_country'] = direct_text(order_origin, 'COUNTRY')
    formatted['order_origin_state'] = direct_text(order_origin, 'STATE')
    formatted['order_origin_postcode'] = direct_text(order_origin, 'POSTCODE')
    formatted['order_origin_geocode'] = direct_text(order_origin, 'GEOCODE')

    ship_from = direct_child(line, 'SHIP_FROM')
    formatted['ship_from_country'] = direct_text(ship_from, 'COUNTRY')
    formatted['ship_from_state'] = direct_text(ship_from, 'STATE')
    formatted['ship_from_postcode'] = direct_text(ship_from, 'POSTCODE')
    formatted['ship_from_geocode'] = direct_text(ship_from, 'GEOCODE')
    formatted['ship_from_location_tax_category'] = direct_text(ship_from, 'LOCATION_TAX_CATEGORY')

    ship_to = direct_child(line, 'SHIP_TO')
    formatted['ship_to_country'] = direct_text(ship_to, 'COUNTRY')
    formatted['ship_to_state'] = direct_text(ship_to, 'STATE')
    formatted['ship_to_postcode'] = direct_text(ship_to, 'POSTCODE')
    formatted['ship_to_geocode'] = direct_text(ship_to, 'GEOCODE')

    exempt_amount = direct_child(line, 'EXEMPT_AMOUNT')
    formatted['exempt_amount_country'] = direct_text(exempt_amount, 'COUNTRY')
    formatted['exempt_amount_province'] = direct_text(exempt_amount, 'PROVINCE')
    formatted['exempt_amount_state'] = direct_text(exempt_amount, 'STATE')
    formatted['exempt_amount_county'] = direct_text(exempt_amount, 'COUNTY')
    formatted['exempt_amount_city'] = direct_text(exempt_amount, 'CITY')
    formatted['exempt_amount_district'] = direct_text(exempt_amount, 'DISTRICT')
    formatted['exempt_amount_postcode'] = direct_text(exempt_amount, 'POSTCODE')
    formatted['exempt_amount_geocode'] = direct_text(exempt_amount, 'GEOCODE')

    quantities = direct_child(line, 'QUANTITIES')
    quantity_block = direct_child(quantities, 'QUANTITY') if quantities is not None else None
    formatted['quantity'] = direct_text(quantity_block, 'AMOUNT')
    formatted['unit_of_measure'] = direct_text(line, 'UNIT_OF_MEASURE')
    if quantity_block is not None and not formatted['unit_of_measure']:
        formatted['unit_of_measure'] = direct_text(quantity_block, 'UOM')
    formatted['supplementary_unit'] = direct_text(line, 'SUPPLEMENTARY_UNIT')

    user_elements = direct_children(line, 'USER_ELEMENT')
    user_lookup = {}
    for user_element in user_elements:
        name = direct_text(user_element, 'NAME').upper()
        value = direct_text(user_element, 'VALUE')
        if name:
            user_lookup[name] = value

    formatted['attribute_1'] = user_lookup.get('ATTRIBUTE1', '')
    formatted['attribute_24'] = user_lookup.get('ATTRIBUTE24', '')
    formatted['attribute_48'] = user_lookup.get('ATTRIBUTE48', '')
    formatted['attribute_2'] = user_lookup.get('ATTRIBUTE2', '')
    formatted['attribute_5'] = user_lookup.get('ATTRIBUTE5', '')
    formatted['attribute_6'] = user_lookup.get('ATTRIBUTE6', '')
    formatted['attribute_9'] = user_lookup.get('ATTRIBUTE9', '')
    formatted['attribute_10'] = user_lookup.get('ATTRIBUTE10', '')
    formatted['attribute_14'] = user_lookup.get('ATTRIBUTE14', '')
    formatted['attribute_18'] = user_lookup.get('ATTRIBUTE18', '')
    formatted['attribute_19'] = user_lookup.get('ATTRIBUTE19', '')
    formatted['attribute_20'] = user_lookup.get('ATTRIBUTE20', '')
    formatted['attribute_22'] = user_lookup.get('ATTRIBUTE22', '')
    formatted['attribute_26'] = user_lookup.get('ATTRIBUTE26', '')
    formatted['attribute_32'] = user_lookup.get('ATTRIBUTE32', '')
    formatted['attribute_37'] = user_lookup.get('ATTRIBUTE37', '')
    formatted['attribute_39'] = user_lookup.get('ATTRIBUTE39', '')

    tax_summary = direct_child(line, 'TAX_SUMMARY')
    if tax_summary is not None:
        formatted['tax_summary_taxable_basis'] = direct_text(tax_summary, 'TAXABLE_BASIS')
        formatted['tax_summary_non_taxable_basis'] = direct_text(tax_summary, 'NON_TAXABLE_BASIS')
        formatted['tax_summary_exempt_amount'] = direct_text(tax_summary, 'EXEMPT_AMOUNT')
        formatted['tax_summary_tax_rate'] = direct_text(tax_summary, 'TAX_RATE')
        formatted['tax_summary_effective_tax_rate'] = direct_text(tax_summary, 'EFFECTIVE_TAX_RATE')

        advisories = []
        for message in direct_children(tax_summary, 'MESSAGE'):
            description = direct_text(message, 'DESCRIPTION')
            if description:
                advisories.append(description)
        formatted['tax_summary_advisory'] = ' | '.join(advisories)

    return formatted

# ============================================================================
# SOAP REQUEST BUILDER
# ============================================================================

def build_soap_request(row_data):
    """
    Build a SOAP XML request from one row or a list of rows.
    Returns the complete SOAP envelope as a string.
    When multiple rows are provided, a single INVOICE is created with multiple LINE nodes.
    """
    try:
        rows = row_data if isinstance(row_data, list) else [row_data]
        header_row = rows[0]

        def get_value(source_row, *keys, default=''):
            """Return first non-empty value across aliases; fall back to defaults for canonical key."""
            for key in keys:
                if key in source_row and source_row.get(key) is not None:
                    value = str(source_row.get(key)).strip()
                    if value and value.lower() != 'nan':
                        return value

            if keys:
                canonical = keys[0]
                default_value = DEFAULTS.get(canonical, default)
                return str(default_value) if default_value is not None else ''
            return str(default)

        def get_row_value(*keys, default=''):
            return get_value(header_row, *keys, default=default)

        def val(*keys, default=''):
            return xml_escape(get_row_value(*keys, default=default))

        def row_val(source_row, *keys, default=''):
            return xml_escape(get_value(source_row, *keys, default=default))

        def date_minus_one_yyyymmdd(value):
            if value and len(value) == 8 and value.isdigit():
                try:
                    parsed = datetime.strptime(value, '%Y%m%d')
                    return (parsed.fromordinal(parsed.toordinal() - 1)).strftime('%Y%m%d')
                except ValueError:
                    return value
            return value

        def normalized_calling_system_number():
            raw = get_row_value('CALLING_SYSTEM_NUMBER', 'calling_system_number').strip()
            if raw.isdigit():
                return raw.zfill(3)
            return raw

        external_company_id_raw = get_row_value('EXTERNAL_COMPANY_ID', 'external_company_id').strip().upper()
        currency_code_raw = get_row_value('CURRENCY_CODE', 'currency_code').strip().upper()
        currency_code = currency_code_raw or ('CAD' if external_company_id_raw.startswith('BFCA') else 'USD')
        company_role = get_row_value('COMPANY_ROLE', 'company_role', default='S').strip() or 'S'
        host_system = get_row_value('HOST_SYSTEM', 'host_system').strip() or 'BFP'
        calling_system_number = normalized_calling_system_number()

        request_document_id_raw = get_row_value(
            'REQUEST_DOCUMENT_ID',
            'request_document_id',
            default='',
        ).strip()
        request_document_id = normalize_request_document_id(request_document_id_raw)

        invoice_number = get_row_value(
            'INVOICE_NUMBER',
            'invoice_number',
        ).strip()
        invoice_date_raw = get_row_value('INVOICE_HEADER_DATE', 'invoice_header_date', 'INVOICE_DATE', 'invoice_date').strip()
        invoice_tax_determination_date = get_row_value(
            'INVOICE_TAX_DETERMINATION_DATE',
            'invoice_tax_determination_date',
            'TAX_DETERMINATION_DATE',
            'tax_determination_date',
            default=invoice_date_raw,
        ).strip() or invoice_date_raw
        invoice_transaction_type = get_row_value(
            'INVOICE_TRANSACTION_TYPE',
            'invoice_transaction_type',
            'TRANSACTION_TYPE',
            'transaction_type',
            default='GS',
        ).strip() or 'GS'

        original_invoice_number_raw = get_row_value('ORIGINAL_INVOICE_NUMBER', 'original_invoice_number').strip()
        unique_invoice_number_raw = get_row_value('UNIQUE_INVOICE_NUMBER', 'unique_invoice_number').strip()

        # Keep invoice identity dynamic per row while preserving the expected format.
        unique_invoice_number = (
            unique_invoice_number_raw
            or (f"{external_company_id_raw}|{invoice_number}|{company_role}" if invoice_number else f"{external_company_id_raw}|")
        )

        def location_value(source_row, primary, fallback, default=''):
            value = get_value(source_row, primary, fallback, default=default).strip()
            return value

        original_invoice_xml = (
            f"<ORIGINAL_INVOICE_NUMBER>{xml_escape(original_invoice_number_raw)}</ORIGINAL_INVOICE_NUMBER>"
            if original_invoice_number_raw else
            "<ORIGINAL_INVOICE_NUMBER/>"
        )

        def fmt_attribute(name, value):
            text = '' if value is None else str(value).strip()
            if not text or text.lower() == 'nan':
                return ''

            # Preserve common formatting shown in the reference SOAP payload.
            if name == 'ATTRIBUTE6':
                try:
                    return f"{float(text):8.2f}"
                except ValueError:
                    return text
            if name == 'ATTRIBUTE14':
                try:
                    return f"{float(text):.3f}"
                except ValueError:
                    return text
            if name == 'ATTRIBUTE18':
                return text.rjust(24)
            if name == 'ATTRIBUTE22':
                return text.zfill(2)
            if name == 'ATTRIBUTE48':
                try:
                    return f"{float(text):.2f}"
                except ValueError:
                    return text

            return text

        def build_user_elements(source_row):
            attributes = {
                name: get_value(source_row, *aliases)
                for name, aliases in USER_ELEMENT_MAPPING.items()
            }

            user_element_parts = []
            for name, value in attributes.items():
                text = fmt_attribute(name, value)
                if not text:
                    continue
                user_element_parts.append(
                    f"""
                  <USER_ELEMENT>
                     <NAME>{name}</NAME>
                     <VALUE>{xml_escape(text)}</VALUE>
                  </USER_ELEMENT>"""
                )
            return ''.join(user_element_parts)

        def line_sort_key(source_row, fallback_index):
            raw = get_value(source_row, 'LINE_NUMBER', 'line_number', default='').strip()
            if raw.isdigit():
                return (0, int(raw), fallback_index)
            return (1, raw, fallback_index)

        lines_xml_parts = []
        sorted_rows = sorted(enumerate(rows, start=1), key=lambda item: line_sort_key(item[1], item[0]))
        for sequence, source_row in sorted_rows:
            line_number = get_value(source_row, 'LINE_NUMBER', 'line_number', default=str(sequence)).strip() or str(sequence)
            line_id = get_value(source_row, 'LINE_ID', 'line_id', default=line_number).strip() or line_number
            part_number_raw = get_value(source_row, 'PART_NUMBER', 'part_number').strip()
            padded_part_number = part_number_raw.zfill(18) if part_number_raw else ''

            # Match expected geocode rendering where missing/0 geocode is sent as 0000.
            ship_to_geocode_raw = get_value(source_row, 'SHIP_TO_GEOCODE', 'ship_to_geocode').strip()
            ship_to_geocode = '0000' if ship_to_geocode_raw in ('', '0', '0.0') else ship_to_geocode_raw

            supplementary_unit = get_value(source_row, 'SUPPLEMENTARY_UNIT', 'supplementary_unit').strip()
            unit_of_measure = get_value(source_row, 'UNIT_OF_MEASURE', 'unit_of_measure', default='EA').strip() or 'EA'
            if not supplementary_unit:
                supplementary_unit = unit_of_measure

            point_of_title_transfer = get_value(source_row, 'POINT_OF_TITLE_TRANSFER', 'point_of_title_transfer').strip()
            title_transfer_location = get_value(source_row, 'TITLE_TRANSFER_LOCATION', 'title_transfer_location').strip()
            product_code = get_value(source_row, 'PRODUCT_CODE', 'product_code').strip()
            inclusive_tax_indicators = get_value(source_row, 'INCLUSIVE_TAX_INDICATORS', 'inclusive_tax_indicators').strip()
            regime = get_value(source_row, 'REGIME', 'regime', default='1').strip() or '1'

            order_acceptance_country = location_value(source_row, 'ORDER_ACCEPTANCE_COUNTRY', 'order_acceptance_country', default='US') or 'US'
            order_acceptance_state = location_value(source_row, 'ORDER_ACCEPTANCE_STATE', 'order_acceptance_state', default='IL') or 'IL'
            order_acceptance_postcode = location_value(source_row, 'ORDER_ACCEPTANCE_POSTCODE', 'order_acceptance_postcode', default='60517') or '60517'
            order_acceptance_geocode = location_value(source_row, 'ORDER_ACCEPTANCE_GEOCODE', 'order_acceptance_geocode', default='4801') or '4801'

            order_origin_country = location_value(source_row, 'ORDER_ORIGIN_COUNTRY', 'order_origin_country', default='US') or 'US'
            order_origin_state = location_value(source_row, 'ORDER_ORIGIN_STATE', 'order_origin_state', default='IL') or 'IL'
            order_origin_postcode = location_value(source_row, 'ORDER_ORIGIN_POSTCODE', 'order_origin_postcode', default='60517') or '60517'
            order_origin_geocode = location_value(source_row, 'ORDER_ORIGIN_GEOCODE', 'order_origin_geocode', default='4801') or '4801'

            ship_from_country = location_value(source_row, 'SHIP_FROM_COUNTRY', 'ship_from_country', default='US') or 'US'
            ship_from_state = location_value(source_row, 'SHIP_FROM_STATE', 'ship_from_state', default='IL') or 'IL'
            ship_from_postcode = location_value(source_row, 'SHIP_FROM_POSTCODE', 'ship_from_postcode', default='60517') or '60517'
            ship_from_geocode = location_value(source_row, 'SHIP_FROM_GEOCODE', 'ship_from_geocode', default='4801') or '4801'
            ship_from_location_tax_category = location_value(source_row, 'SHIP_FROM_LOCATION_TAX_CATEGORY', 'ship_from_location_tax_category')

            exempt_amount_country = get_value(source_row, 'EXEMPT_AMOUNT_COUNTRY', 'exempt_amount_country', default='0').strip() or '0'
            exempt_amount_province = get_value(source_row, 'EXEMPT_AMOUNT_PROVINCE', 'exempt_amount_province', default='0').strip() or '0'
            exempt_amount_state = get_value(source_row, 'EXEMPT_AMOUNT_STATE', 'exempt_amount_state', default='0').strip() or '0'
            exempt_amount_county = get_value(source_row, 'EXEMPT_AMOUNT_COUNTY', 'exempt_amount_county', default='0').strip() or '0'
            exempt_amount_city = get_value(source_row, 'EXEMPT_AMOUNT_CITY', 'exempt_amount_city', default='0').strip() or '0'
            exempt_amount_district = get_value(source_row, 'EXEMPT_AMOUNT_DISTRICT', 'exempt_amount_district', default='0').strip() or '0'
            exempt_amount_postcode = get_value(source_row, 'EXEMPT_AMOUNT_POSTCODE', 'exempt_amount_postcode', default='0').strip() or '0'
            exempt_amount_geocode = get_value(source_row, 'EXEMPT_AMOUNT_GEOCODE', 'exempt_amount_geocode', default='0').strip() or '0'

            explicit_line_invoice_date = get_value(source_row, 'LINE_INVOICE_DATE', 'line_invoice_date').strip()
            if explicit_line_invoice_date:
                row_line_invoice_date = explicit_line_invoice_date
            else:
                row_line_invoice_date = date_minus_one_yyyymmdd(invoice_date_raw)

            row_line_tax_determination_date = get_value(
                source_row,
                'LINE_TAX_DETERMINATION_DATE',
                'line_tax_determination_date',
                'TAX_DETERMINATION_DATE',
                'tax_determination_date',
                default=invoice_tax_determination_date,
            ).strip() or invoice_tax_determination_date
            row_line_transaction_type = get_value(
                source_row,
                'LINE_TRANSACTION_TYPE',
                'line_transaction_type',
                'TRANSACTION_TYPE',
                'transaction_type',
                default=invoice_transaction_type,
            ).strip() or invoice_transaction_type

            user_elements = build_user_elements(source_row)

            lines_xml_parts.append(
                f"""
                    <LINE ID=\"{xml_escape(line_id)}\">
                        <COMMODITY_CODE>{row_val(source_row, 'COMMODITY_CODE', 'commodity_code')}</COMMODITY_CODE>
                        <COUNTRY_OF_ORIGIN>{row_val(source_row, 'COUNTRY_OF_ORIGIN', 'country_of_origin')}</COUNTRY_OF_ORIGIN>
                        <CUSTOMER_NUMBER>{row_val(source_row, 'CUSTOMER_NUMBER', 'customer_number')}</CUSTOMER_NUMBER>
                        <DELIVERY_TERMS>{row_val(source_row, 'DELIVERY_TERMS', 'delivery_terms')}</DELIVERY_TERMS>
                        <GROSS_AMOUNT>{row_val(source_row, 'GROSS_AMOUNT', 'gross_amount')}</GROSS_AMOUNT>
                        <IS_BUSINESS_SUPPLY>{row_val(source_row, 'IS_BUSINESS_SUPPLY', 'is_business_supply')}</IS_BUSINESS_SUPPLY>
                        <IS_CREDIT>{row_val(source_row, 'IS_CREDIT', 'is_credit')}</IS_CREDIT>
                        <INVOICE_DATE>{xml_escape(row_line_invoice_date)}</INVOICE_DATE>
                        <LINE_NUMBER>{xml_escape(line_number)}</LINE_NUMBER>
                        <ORDER_ACCEPTANCE>
                            <COUNTRY>{xml_escape(order_acceptance_country)}</COUNTRY>
                            <STATE>{xml_escape(order_acceptance_state)}</STATE>
                            <POSTCODE>{xml_escape(order_acceptance_postcode)}</POSTCODE>
                            <GEOCODE>{xml_escape(order_acceptance_geocode)}</GEOCODE>
                        </ORDER_ACCEPTANCE>
                        <ORDER_ORIGIN>
                            <COUNTRY>{xml_escape(order_origin_country)}</COUNTRY>
                            <STATE>{xml_escape(order_origin_state)}</STATE>
                            <POSTCODE>{xml_escape(order_origin_postcode)}</POSTCODE>
                            <GEOCODE>{xml_escape(order_origin_geocode)}</GEOCODE>
                        </ORDER_ORIGIN>
                        <SHIP_FROM>
                            <COUNTRY>{xml_escape(ship_from_country)}</COUNTRY>
                            <STATE>{xml_escape(ship_from_state)}</STATE>
                            <POSTCODE>{xml_escape(ship_from_postcode)}</POSTCODE>
                            <GEOCODE>{xml_escape(ship_from_geocode)}</GEOCODE>
                            <LOCATION_TAX_CATEGORY>{xml_escape(ship_from_location_tax_category)}</LOCATION_TAX_CATEGORY>
                        </SHIP_FROM>
                        <SHIP_TO>
                            <COUNTRY>{row_val(source_row, 'SHIP_TO_COUNTRY', 'ship_to_country')}</COUNTRY>
                            <STATE>{row_val(source_row, 'SHIP_TO_STATE', 'ship_to_state')}</STATE>
                            <POSTCODE>{row_val(source_row, 'SHIP_TO_POSTCODE', 'ship_to_postcode')}</POSTCODE>
                            <GEOCODE>{xml_escape(ship_to_geocode)}</GEOCODE>
                            <LOCATION_TAX_CATEGORY/>
                        </SHIP_TO>
                        <EXEMPT_AMOUNT>
                            <COUNTRY>{xml_escape(exempt_amount_country)}</COUNTRY>
                            <PROVINCE>{xml_escape(exempt_amount_province)}</PROVINCE>
                            <STATE>{xml_escape(exempt_amount_state)}</STATE>
                            <COUNTY>{xml_escape(exempt_amount_county)}</COUNTY>
                            <CITY>{xml_escape(exempt_amount_city)}</CITY>
                            <DISTRICT>{xml_escape(exempt_amount_district)}</DISTRICT>
                            <POSTCODE>{xml_escape(exempt_amount_postcode)}</POSTCODE>
                            <GEOCODE>{xml_escape(exempt_amount_geocode)}</GEOCODE>
                        </EXEMPT_AMOUNT>
                        <PART_NUMBER>{xml_escape(padded_part_number)}</PART_NUMBER>
                        <POINT_OF_TITLE_TRANSFER>{xml_escape(point_of_title_transfer)}</POINT_OF_TITLE_TRANSFER>
                        <TITLE_TRANSFER_LOCATION>{xml_escape(title_transfer_location)}</TITLE_TRANSFER_LOCATION>
                        <PRODUCT_CODE>{xml_escape(product_code)}</PRODUCT_CODE>
                        <REGIME>{xml_escape(regime)}</REGIME>
                        <SUPPLEMENTARY_UNIT>{xml_escape(supplementary_unit)}</SUPPLEMENTARY_UNIT>
                        <TAX_DETERMINATION_DATE>{xml_escape(row_line_tax_determination_date)}</TAX_DETERMINATION_DATE>
                        <TRANSACTION_TYPE>{xml_escape(row_line_transaction_type)}</TRANSACTION_TYPE>
                        <UNIT_OF_MEASURE>{xml_escape(unit_of_measure)}</UNIT_OF_MEASURE>
                        <INCLUSIVE_TAX_INDICATORS>{xml_escape(inclusive_tax_indicators)}</INCLUSIVE_TAX_INDICATORS>
                        <QUANTITIES>
                            <QUANTITY>
                                <AMOUNT>{row_val(source_row, 'QUANTITY', 'quantity')}</AMOUNT>
                                <UOM>{xml_escape(unit_of_measure)}</UOM>
                            </QUANTITY>
                        </QUANTITIES>
                        <REGISTRATIONS/>{user_elements}
                    </LINE>"""
            )

        lines_xml = ''.join(lines_xml_parts)

        soap_env = f'''<?xml version="1.0" encoding="UTF-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" 
    xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd" 
    xmlns:wsu="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd">
    <soap:Header>
        <wsse:Security soap:mustUnderstand="1">
            <wsse:UsernameToken>
                <wsse:Username>{SOAP_USERNAME}</wsse:Username>
                <wsse:Password Type="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordText">{SOAP_PASSWORD}</wsse:Password>
            </wsse:UsernameToken>
        </wsse:Security>
    </soap:Header>
    <soap:Body>
        <taxCalculationRequest xmlns="http://www.sabrix.com/services/taxcalculationservice/2011-09-01">
            <INDATA version="G">
                <CALLING_SYSTEM_NUMBER>{xml_escape(calling_system_number)}</CALLING_SYSTEM_NUMBER>
                <HOST_SYSTEM>{xml_escape(host_system)}</HOST_SYSTEM>
                <INVOICE>
                    <COMPANY_ROLE>S</COMPANY_ROLE>
                    <CURRENCY_CODE>{xml_escape(currency_code)}</CURRENCY_CODE>
                    <EXTERNAL_COMPANY_ID>1003994714-BFS1</EXTERNAL_COMPANY_ID>
                    <INVOICE_DATE>{xml_escape(invoice_date_raw)}</INVOICE_DATE>
                    <INVOICE_NUMBER>{xml_escape(invoice_number)}</INVOICE_NUMBER>
                    <IS_AUDITED>N</IS_AUDITED>
                    <IS_ROUNDING>{val('IS_ROUNDING', 'is_rounding')}</IS_ROUNDING>
                    {original_invoice_xml}
                    <TAX_DETERMINATION_DATE>{xml_escape(invoice_tax_determination_date)}</TAX_DETERMINATION_DATE>
                    <TRANSACTION_TYPE>{xml_escape(invoice_transaction_type)}</TRANSACTION_TYPE>
                    <UNIQUE_INVOICE_NUMBER>{xml_escape(unique_invoice_number)}</UNIQUE_INVOICE_NUMBER>
{lines_xml}
                </INVOICE>
            </INDATA>
        </taxCalculationRequest>
    </soap:Body>
</soap:Envelope>'''
        return soap_env
    
    except Exception as e:
        if isinstance(row_data, list):
            invoice_for_log = 'grouped_document'
        else:
            invoice_for_log = row_data.get('INVOICE_NUMBER', 'unknown')
        logger.error(f"Error building SOAP request for row {invoice_for_log}: {str(e)}")
        return None

# ============================================================================
# SOAP REQUEST SENDER
# ============================================================================

def send_soap_request(row_data, row_index):
    """
    Send SOAP request to endpoint and capture response.
    Returns a dict with request/response details.
    """
    request_document_id = normalize_request_document_id(
        row_data.get('REQUEST_DOCUMENT_ID') or row_data.get('request_document_id')
    )

    invoice_num = (
        request_document_id
        or row_data.get('ORIGINAL_INVOICE_NUMBER')
        or row_data.get('original_invoice_number')
        or row_data.get('INVOICE_NUMBER')
        or row_data.get('invoice_number')
        or 'unknown'
    )
    result = {
        'row_index': row_index,
        'invoice_number': invoice_num,
        'status': 'pending',
        'http_status': None,
        'response_time_ms': 0,
        'error_message': '',
        'error_type': '',
        'error_details': '',
        'is_success_response': None,
        'total_tax': None,
        'response_excerpt': '',
        'outdata_mapped_json': '',
        'outdata_formatted_row': empty_outdata_formatted_row(row_data),
    }
    
    try:
        # Build SOAP request
        soap_request = build_soap_request(row_data)
        if not soap_request:
            result['status'] = 'failed'
            result['error_message'] = 'Failed to build SOAP request'
            return result
        
        # Send request
        headers = {
            'Content-Type': 'text/xml; charset=utf-8',
            'SOAPAction': SOAP_ACTION
        }
        
        start_time = time.time()
        response = requests.post(
            SOAP_ENDPOINT,
            data=soap_request,
            headers=headers,
            timeout=REQUEST_TIMEOUT,
            verify=False  # Disable SSL verification (use with caution in production)
        )
        elapsed_ms = (time.time() - start_time) * 1000
        
        result['http_status'] = response.status_code
        result['response_time_ms'] = round(elapsed_ms, 2)
        
        # Parse response
        if response.status_code == 200:
            try:
                # Parse XML response
                root = parse_response_xml(response.content)
                
                # Extract key fields from response

                # Check for success
                is_success = find_first_by_local_name(root, 'IS_SUCCESS')
                if is_success is not None and is_success.text is not None:
                    result['is_success_response'] = is_success.text.strip().lower() == 'true'
                
                # Extract total tax
                total_tax = find_first_by_local_name(root, 'TOTAL_TAX_AMOUNT')
                if total_tax is not None and total_tax.text is not None:
                    result['total_tax'] = total_tax.text.strip()

                # Extract complete OUTDATA mapping, excluding all MESSAGE blocks
                outdata_mapping = extract_outdata_mapping(root)
                if outdata_mapping is not None:
                    result['outdata_mapped_json'] = json.dumps(outdata_mapping, ensure_ascii=False)
                    result['outdata_formatted_row'] = extract_outdata_formatted_row(root, row_data)
                else:
                    # Helps diagnose responses that are valid SOAP Faults or alternate payloads.
                    result['error_message'] = 'OUTDATA block not found in SOAP response'
                
                # Get response excerpt (first 500 chars of body)
                body = root.find('.//soap:Body', SOAP_NAMESPACES)
                if body is not None:
                    result['response_excerpt'] = ET.tostring(body, encoding='unicode')[:500]
                
                result['status'] = 'success'
                logger.info(f"Row {row_index}: {invoice_num} - Status: {result['http_status']}, Time: {result['response_time_ms']}ms")
                
            except ET.ParseError as e:
                result['status'] = 'failed'
                result['error_message'] = f'Failed to parse XML response: {str(e)}'
                result.update(exception_details(e))
                result['response_excerpt'] = trimmed_text(response.text, limit=2000)
                logger.error(f"Row {row_index}: {invoice_num} - XML Parse Error: {str(e)}")
        elif response.status_code == 500:
            result['status'] = 'failed'
            result['error_message'] = 'HTTP 500: SOAP server returned a failure response'
            result['error_type'] = 'HTTP500'

            # Try to parse SOAP Fault details for diagnostics only.
            try:
                root = parse_response_xml(response.content)
                fault = find_first_by_local_name(root, 'Fault')
                if fault is not None:
                    fault_string = find_first_by_local_name(fault, 'faultstring')
                    if fault_string is not None and fault_string.text:
                        result['error_message'] = f"HTTP 500 SOAP Fault: {fault_string.text.strip()}"

                body = root.find('.//soap:Body', SOAP_NAMESPACES)
                if body is not None:
                    result['response_excerpt'] = ET.tostring(body, encoding='unicode')[:500]
            except ET.ParseError:
                # Keep the generic 500 error if fault parsing also fails.
                pass

            if not result['response_excerpt']:
                result['response_excerpt'] = trimmed_text(response.text, limit=2000)

            logger.error(f"Row {row_index}: {invoice_num} - HTTP 500 failure")
        else:
            result['status'] = 'failed'
            result['error_type'] = f'HTTP{response.status_code}'
            result['error_message'] = f'HTTP {response.status_code}: {trimmed_text(response.text, limit=400)}'
            result['response_excerpt'] = trimmed_text(response.text, limit=2000)
            logger.error(f"Row {row_index}: {invoice_num} - HTTP Error {response.status_code}")
    
    except requests.Timeout:
        result['status'] = 'failed'
        result['error_message'] = f'Request timeout after {REQUEST_TIMEOUT}s'
        result['error_type'] = 'Timeout'
        logger.error(f"Row {row_index}: {invoice_num} - Timeout")
    except requests.RequestException as e:
        result['status'] = 'failed'
        result['error_message'] = f'Request failed: {str(e)}'
        result.update(exception_details(e))
        logger.error(f"Row {row_index}: {invoice_num} - {str(e)}")
    except Exception as e:
        result['status'] = 'failed'
        result['error_message'] = f'Unexpected error: {str(e)}'
        result.update(exception_details(e))
        logger.error(f"Row {row_index}: {invoice_num} - Unexpected error: {str(e)}")
    
    return result


def send_soap_request_group(group_rows, group_row_indexes):
    """
    Send one SOAP request for all rows in the same document group.
    Returns one result dict per input row index.
    """
    first_row = group_rows[0]
    request_document_id = normalize_request_document_id(
        first_row.get('REQUEST_DOCUMENT_ID') or first_row.get('request_document_id')
    )

    invoice_num = (
        request_document_id
        or first_row.get('ORIGINAL_INVOICE_NUMBER')
        or first_row.get('original_invoice_number')
        or first_row.get('INVOICE_NUMBER')
        or first_row.get('invoice_number')
        or 'unknown'
    )

    template = {
        'invoice_number': invoice_num,
        'status': 'pending',
        'http_status': None,
        'response_time_ms': 0,
        'error_message': '',
        'error_type': '',
        'error_details': '',
        'is_success_response': None,
        'total_tax': None,
        'response_excerpt': '',
        'outdata_mapped_json': ''
    }

    try:
        soap_request = build_soap_request(group_rows)
        if not soap_request:
            failed = []
            for row_index in group_row_indexes:
                result = dict(template)
                result['row_index'] = row_index
                result['status'] = 'failed'
                result['error_message'] = 'Failed to build SOAP request'
                failed.append(result)
            return failed

        headers = {
            'Content-Type': 'text/xml; charset=utf-8',
            'SOAPAction': SOAP_ACTION
        }

        start_time = time.time()
        response = requests.post(
            SOAP_ENDPOINT,
            data=soap_request,
            headers=headers,
            timeout=REQUEST_TIMEOUT,
            verify=False
        )
        elapsed_ms = (time.time() - start_time) * 1000

        base = dict(template)
        base['http_status'] = response.status_code
        base['response_time_ms'] = round(elapsed_ms, 2)

        if response.status_code == 200:
            try:
                root = parse_response_xml(response.content)

                is_success = find_first_by_local_name(root, 'IS_SUCCESS')
                if is_success is not None and is_success.text is not None:
                    base['is_success_response'] = is_success.text.strip().lower() == 'true'

                total_tax = find_first_by_local_name(root, 'TOTAL_TAX_AMOUNT')
                if total_tax is not None and total_tax.text is not None:
                    base['total_tax'] = total_tax.text.strip()

                outdata_mapping = extract_outdata_mapping(root)
                if outdata_mapping is not None:
                    base['outdata_mapped_json'] = json.dumps(outdata_mapping, ensure_ascii=False)
                else:
                    base['error_message'] = 'OUTDATA block not found in SOAP response'

                body = root.find('.//soap:Body', SOAP_NAMESPACES)
                if body is not None:
                    base['response_excerpt'] = ET.tostring(body, encoding='unicode')[:500]

                base['status'] = 'success'
                logger.info(
                    f"Document {invoice_num}: {len(group_rows)} lines - Status: {base['http_status']}, Time: {base['response_time_ms']}ms"
                )

            except ET.ParseError as e:
                base['status'] = 'failed'
                base['error_message'] = f'Failed to parse XML response: {str(e)}'
                base.update(exception_details(e))
                base['response_excerpt'] = trimmed_text(response.text, limit=2000)
                logger.error(f"Document {invoice_num} - XML Parse Error: {str(e)}")
        elif response.status_code == 500:
            base['status'] = 'failed'
            base['error_message'] = 'HTTP 500: SOAP server returned a failure response'
            base['error_type'] = 'HTTP500'

            try:
                root = parse_response_xml(response.content)
                fault = find_first_by_local_name(root, 'Fault')
                if fault is not None:
                    fault_string = find_first_by_local_name(fault, 'faultstring')
                    if fault_string is not None and fault_string.text:
                        base['error_message'] = f"HTTP 500 SOAP Fault: {fault_string.text.strip()}"

                body = root.find('.//soap:Body', SOAP_NAMESPACES)
                if body is not None:
                    base['response_excerpt'] = ET.tostring(body, encoding='unicode')[:500]
            except ET.ParseError:
                pass

            if not base['response_excerpt']:
                base['response_excerpt'] = trimmed_text(response.text, limit=2000)

            logger.error(f"Document {invoice_num} - HTTP 500 failure")
        else:
            base['status'] = 'failed'
            base['error_type'] = f'HTTP{response.status_code}'
            base['error_message'] = f'HTTP {response.status_code}: {trimmed_text(response.text, limit=400)}'
            base['response_excerpt'] = trimmed_text(response.text, limit=2000)
            logger.error(f"Document {invoice_num} - HTTP Error {response.status_code}")

        results = []
        for row_index in group_row_indexes:
            row_result = dict(base)
            row_result['row_index'] = row_index
            results.append(row_result)
        return results

    except requests.Timeout:
        results = []
        for row_index in group_row_indexes:
            row_result = dict(template)
            row_result['row_index'] = row_index
            row_result['status'] = 'failed'
            row_result['error_message'] = f'Request timeout after {REQUEST_TIMEOUT}s'
            row_result['error_type'] = 'Timeout'
            results.append(row_result)
        logger.error(f"Document {invoice_num} - Timeout")
        return results
    except requests.RequestException as e:
        results = []
        for row_index in group_row_indexes:
            row_result = dict(template)
            row_result['row_index'] = row_index
            row_result['status'] = 'failed'
            row_result['error_message'] = f'Request failed: {str(e)}'
            row_result.update(exception_details(e))
            results.append(row_result)
        logger.error(f"Document {invoice_num} - {str(e)}")
        return results
    except Exception as e:
        results = []
        for row_index in group_row_indexes:
            row_result = dict(template)
            row_result['row_index'] = row_index
            row_result['status'] = 'failed'
            row_result['error_message'] = f'Unexpected error: {str(e)}'
            row_result.update(exception_details(e))
            results.append(row_result)
        logger.error(f"Document {invoice_num} - Unexpected error: {str(e)}")
        return results

# ============================================================================
# MAIN PROCESSING
# ============================================================================

def parse_args():
    """Parse CLI options for batch mode and load-test mode."""
    parser = argparse.ArgumentParser(description='SOAP tax processor and load test utility')
    parser.add_argument('--mode', choices=['batch', 'load-test'], default='batch')
    parser.add_argument('--input-file', default=INPUT_FILE)
    parser.add_argument('--output-file', default=OUTPUT_FILE)
    parser.add_argument('--outdata-output-file', default=OUTDATA_OUTPUT_FILE)
    parser.add_argument('--max-workers', type=int, default=MAX_WORKERS)
    parser.add_argument(
        '--request-granularity',
        choices=['document', 'row'],
        default='row',
        help='row: one SOAP call per input row (default); document: one SOAP call per REQUEST_DOCUMENT_ID group'
    )

    parser.add_argument('--load-requests', type=int, default=1000, help='Total number of SOAP calls to execute')
    parser.add_argument('--load-concurrency', type=int, default=1000, help='Concurrent workers for load test')
    parser.add_argument('--load-source', choices=['cycle', 'first'], default='cycle')
    parser.add_argument('--load-results-file', default=LOAD_TEST_RESULTS_FILE)
    parser.add_argument('--load-summary-file', default=LOAD_TEST_SUMMARY_FILE)
    parser.add_argument('--load-failures-file', default=LOAD_TEST_FAILURES_FILE)
    return parser.parse_args()


def load_input_dataframe(input_file):
    """Read CSV/XLSX into a string-typed dataframe."""
    logger.info(f"Loading input file: {input_file}")
    if input_file.endswith('.xlsx'):
        return pd.read_excel(input_file, dtype=str)
    return pd.read_csv(input_file, dtype=str, keep_default_na=False)


def group_documents(df):
    """Group rows by REQUEST_DOCUMENT_ID to send one SOAP call per document."""
    grouped_documents = {}
    for idx, (_, row) in enumerate(df.iterrows()):
        row_dict = row.to_dict()
        document_id = normalize_request_document_id(
            row_dict.get('REQUEST_DOCUMENT_ID') or row_dict.get('request_document_id')
        )
        group_key = document_id if document_id else f"__ROW_{idx}"

        if group_key not in grouped_documents:
            grouped_documents[group_key] = {
                'rows': [],
                'row_indexes': []
            }

        grouped_documents[group_key]['rows'].append(row_dict)
        grouped_documents[group_key]['row_indexes'].append(idx)
    return grouped_documents

def process_batch(
    input_file,
    output_file=OUTPUT_FILE,
    outdata_output_file=OUTDATA_OUTPUT_FILE,
    max_workers=MAX_WORKERS,
    request_granularity='row',
):
    """
    Load input file, process rows in parallel, and save results.
    """
    logger.info("=" * 80)
    logger.info("SOAP BATCH TAX PROCESSOR - START")
    logger.info("=" * 80)
    
    try:
        # Load input data
        df = load_input_dataframe(input_file)
        
        logger.info(f"Loaded {len(df)} rows")
        
        # Prepare output directory
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        os.makedirs(os.path.dirname(outdata_output_file), exist_ok=True)

        results = []
        if request_granularity == 'row':
            logger.info(
                f"Starting per-row batch processing with {max_workers} workers "
                f"({len(df)} SOAP calls for {len(df)} rows)..."
            )

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {
                    executor.submit(send_soap_request, row.to_dict(), idx): idx
                    for idx, (_, row) in enumerate(df.iterrows())
                }

                completed = 0
                for future in as_completed(futures):
                    results.append(future.result())
                    completed += 1
                    if completed % 10 == 0:
                        logger.info(f"Progress: {completed}/{len(df)} row calls processed")
        else:
            grouped_documents = group_documents(df)

            # Process grouped documents in parallel
            logger.info(
                f"Starting grouped batch processing with {max_workers} workers "
                f"({len(grouped_documents)} document calls for {len(df)} rows)..."
            )

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit one task per document group
                futures = {
                    executor.submit(
                        send_soap_request_group,
                        payload['rows'],
                        payload['row_indexes']
                    ): key
                    for key, payload in grouped_documents.items()
                }

                # Collect results as they complete
                completed = 0
                for future in as_completed(futures):
                    group_results = future.result()
                    results.extend(group_results)
                    completed += 1

                    if completed % 10 == 0:
                        logger.info(f"Progress: {completed}/{len(grouped_documents)} document calls processed")
        
        # Sort results by row index
        results.sort(key=lambda x: x['row_index'])

        # Build output rows.
        results_df = pd.DataFrame(results)
        if request_granularity == 'row':
            if not results_df.empty:
                output_df = results_df.loc[:, [
                    'row_index',
                    'invoice_number',
                    'status',
                    'http_status',
                    'is_success_response',
                    'total_tax',
                    'response_time_ms',
                    'error_type',
                    'error_message',
                    'error_details',
                ]].rename(columns={'is_success_response': 'is_success'})

                outdata_rows = []
                for record in results:
                    formatted = record.get('outdata_formatted_row') or empty_outdata_formatted_row()
                    row_outdata = {
                        'row_index': record.get('row_index', ''),
                        'invoice_number': record.get('invoice_number', ''),
                        'status': record.get('status', ''),
                        'http_status': record.get('http_status', ''),
                    }
                    for column in OUTDATA_FORMATTED_COLUMNS:
                        row_outdata[column] = formatted.get(column, '')
                    row_outdata['outdata_mapped_json'] = record.get('outdata_mapped_json', '')
                    outdata_rows.append(row_outdata)

                outdata_df = pd.DataFrame(outdata_rows)
            else:
                output_df = pd.DataFrame(columns=[
                    'row_index',
                    'invoice_number',
                    'status',
                    'http_status',
                    'is_success',
                    'total_tax',
                    'response_time_ms',
                    'error_type',
                    'error_message',
                    'error_details',
                ])
                outdata_df = pd.DataFrame(columns=[
                    'row_index',
                    'invoice_number',
                    'status',
                    'http_status',
                    *OUTDATA_FORMATTED_COLUMNS,
                    'outdata_mapped_json',
                ])
        else:
            if not results_df.empty:
                output_df = (
                    results_df
                    .groupby('invoice_number', as_index=False)
                    .first()
                    .loc[:, ['invoice_number', 'status', 'http_status', 'is_success_response', 'total_tax']]
                    .rename(columns={'is_success_response': 'is_success'})
                )

                outdata_df = (
                    results_df
                    .groupby('invoice_number', as_index=False)
                    .first()
                    .loc[:, ['invoice_number', 'outdata_mapped_json']]
                )
            else:
                output_df = pd.DataFrame(columns=['invoice_number', 'status', 'http_status', 'is_success', 'total_tax'])
                outdata_df = pd.DataFrame(columns=['invoice_number', 'outdata_mapped_json'])

        output_df.to_csv(output_file, index=False)
        outdata_df.to_csv(outdata_output_file, index=False)
        logger.info(f"Results saved to: {output_file}")
        logger.info(f"OUTDATA saved to: {outdata_output_file}")
        
        # Print summary
        successful = len([r for r in results if r['status'] == 'success'])
        failed = len([r for r in results if r['status'] == 'failed'])
        
        logger.info("=" * 80)
        logger.info("PROCESSING COMPLETE")
        logger.info(f"  Total rows: {len(results)}")
        logger.info(f"  Successful: {successful}")
        logger.info(f"  Failed: {failed}")
        logger.info(f"  Success rate: {(successful/len(results)*100):.1f}%")
        logger.info("=" * 80)
        
        return output_df
    
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}", exc_info=True)
        raise


def run_load_test(
    input_file,
    total_requests=1000,
    concurrency=1000,
    source_mode='cycle',
    results_file=LOAD_TEST_RESULTS_FILE,
    summary_file=LOAD_TEST_SUMMARY_FILE,
    failures_file=LOAD_TEST_FAILURES_FILE,
):
    """Execute high-concurrency SOAP load test and capture exact per-request failures."""
    logger.info("=" * 80)
    logger.info("SOAP LOAD TEST - START")
    logger.info("=" * 80)

    if total_requests <= 0:
        raise ValueError('load-requests must be greater than zero')
    if concurrency <= 0:
        raise ValueError('load-concurrency must be greater than zero')

    df = load_input_dataframe(input_file)
    if df.empty:
        raise ValueError('Input file has no rows; cannot run load test')

    grouped_documents = group_documents(df)
    if not grouped_documents:
        raise ValueError('No grouped documents found in input data')

    groups = list(grouped_documents.items())
    if source_mode == 'first':
        selected_groups = [groups[0][1] for _ in range(total_requests)]
    else:
        selected_groups = [groups[i % len(groups)][1] for i in range(total_requests)]

    os.makedirs(os.path.dirname(results_file), exist_ok=True)
    os.makedirs(os.path.dirname(summary_file), exist_ok=True)
    os.makedirs(os.path.dirname(failures_file), exist_ok=True)

    logger.info(
        f"Starting load test: total_requests={total_requests}, concurrency={concurrency}, "
        f"source_mode={source_mode}, source_documents={len(grouped_documents)}"
    )

    start_event = threading.Event()
    load_results = []
    launched = 0

    def fire_request(request_id, payload):
        # Synchronized release helps approximate "all at once" startup pressure.
        start_event.wait()
        result_list = send_soap_request_group(payload['rows'], [request_id])
        result = result_list[0] if result_list else {
            'row_index': request_id,
            'invoice_number': 'unknown',
            'status': 'failed',
            'http_status': None,
            'response_time_ms': 0,
            'error_message': 'No result returned',
            'error_type': 'NoResult',
            'error_details': '',
            'is_success_response': None,
            'total_tax': None,
            'response_excerpt': '',
            'outdata_mapped_json': ''
        }
        result['request_id'] = request_id
        result['source_row_count'] = len(payload['rows'])
        return result

    start_wall = time.time()
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = {}
        for request_id, payload in enumerate(selected_groups, start=1):
            future = executor.submit(fire_request, request_id, payload)
            futures[future] = request_id
            launched += 1

        logger.info(f"Queued {launched} requests. Releasing workers now...")
        start_event.set()

        completed = 0
        for future in as_completed(futures):
            result = future.result()
            load_results.append(result)
            completed += 1

            if completed % 100 == 0 or completed == total_requests:
                logger.info(f"Load progress: {completed}/{total_requests} requests completed")

    duration_s = time.time() - start_wall
    load_results.sort(key=lambda x: x.get('request_id', 0))
    results_df = pd.DataFrame(load_results)
    results_df.to_csv(results_file, index=False)

    failed_df = results_df[results_df['status'] != 'success'].copy() if not results_df.empty else pd.DataFrame()
    failed_df.to_csv(failures_file, index=False)

    latencies = [float(v) for v in results_df['response_time_ms'].fillna(0).tolist()] if not results_df.empty else []
    status_counts = Counter(str(v) for v in results_df['http_status'].fillna('None').tolist())
    error_type_counts = Counter(str(v) for v in results_df['error_type'].fillna('').tolist() if str(v).strip())

    summary = {
        'timestamp_utc': datetime.utcnow().isoformat() + 'Z',
        'input_file': input_file,
        'total_requests': int(total_requests),
        'concurrency': int(concurrency),
        'source_mode': source_mode,
        'source_documents': int(len(grouped_documents)),
        'duration_seconds': round(duration_s, 3),
        'throughput_rps': round((total_requests / duration_s), 3) if duration_s > 0 else 0.0,
        'success_count': int((results_df['status'] == 'success').sum()) if not results_df.empty else 0,
        'failed_count': int((results_df['status'] != 'success').sum()) if not results_df.empty else 0,
        'failure_rate_percent': round(
            ((results_df['status'] != 'success').sum() / len(results_df) * 100.0), 3
        ) if len(results_df) else 0.0,
        'http_status_counts': dict(status_counts),
        'error_type_counts': dict(error_type_counts),
        'latency_ms': {
            'min': round(min(latencies), 3) if latencies else 0.0,
            'avg': round(sum(latencies) / len(latencies), 3) if latencies else 0.0,
            'p50': round(percentile(latencies, 50), 3) if latencies else 0.0,
            'p90': round(percentile(latencies, 90), 3) if latencies else 0.0,
            'p95': round(percentile(latencies, 95), 3) if latencies else 0.0,
            'p99': round(percentile(latencies, 99), 3) if latencies else 0.0,
            'max': round(max(latencies), 3) if latencies else 0.0,
        },
        'results_file': results_file,
        'failures_file': failures_file,
    }

    with open(summary_file, 'w', encoding='utf-8') as fh:
        json.dump(summary, fh, indent=2)

    logger.info("=" * 80)
    logger.info("SOAP LOAD TEST - COMPLETE")
    logger.info(f"  Requests: {total_requests}")
    logger.info(f"  Failed: {summary['failed_count']}")
    logger.info(f"  Failure rate: {summary['failure_rate_percent']}%")
    logger.info(f"  Throughput: {summary['throughput_rps']} req/s")
    logger.info(f"  p95 latency: {summary['latency_ms']['p95']} ms")
    logger.info(f"  Results CSV: {results_file}")
    logger.info(f"  Failures CSV: {failures_file}")
    logger.info(f"  Summary JSON: {summary_file}")
    logger.info("=" * 80)

    return summary

# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == '__main__':
    ensure_project_venv(Path(__file__).resolve().parents[1])

    args = parse_args()

    if not os.path.exists(args.input_file):
        logger.error(f"Input file not found: {args.input_file}")
        logger.error(f"Please ensure your CSV/Excel file is at: {os.path.abspath(args.input_file)}")
        print(f"\nInput file not found: {args.input_file}")
        print(f"Please save your Alteryx output to: {os.path.abspath(args.input_file)}")
    elif args.mode == 'load-test':
        summary = run_load_test(
            input_file=args.input_file,
            total_requests=args.load_requests,
            concurrency=args.load_concurrency,
            source_mode=args.load_source,
            results_file=args.load_results_file,
            summary_file=args.load_summary_file,
            failures_file=args.load_failures_file,
        )
        print("\nLoad test complete")
        print(f"Failed: {summary['failed_count']} / {summary['total_requests']}")
        print(f"Failure rate: {summary['failure_rate_percent']}%")
        print(f"Summary: {args.load_summary_file}")
    else:
        process_batch(
            args.input_file,
            output_file=args.output_file,
            outdata_output_file=args.outdata_output_file,
            max_workers=args.max_workers,
            request_granularity=args.request_granularity,
        )
        print(f"\nProcessing complete! Results saved to: {args.output_file}")
        print(f"Check {LOG_FILE} for detailed logs")
