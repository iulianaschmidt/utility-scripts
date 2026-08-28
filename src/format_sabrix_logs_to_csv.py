#!/usr/bin/env python3
"""
Format SabrixIntegrationServer logs into a CSV shaped like sample_production_data.csv.

The script filters input files by date encoded in file names such as:
- SabrixIntegrationServer.2026-06-15_16.log
- SabrixIntegrationServer.2026-06-15_16.log.zip
"""

import argparse
import codecs
import csv
import json
import re
import zipfile
from decimal import Decimal, InvalidOperation
from datetime import date, datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple

DEFAULT_INPUT_DIR = Path(r"X:\bfp\isbfp1\sabrix_log")
DEFAULT_OUTPUT_DIR = Path("output")
DEFAULT_CONFIG_FILE = Path(__file__).with_name("format_sabrix_logs_to_csv_config.json")

FILE_DATE_RE = re.compile(
    r".+\.(?P<date>\d{4}-\d{2}-\d{2})_\d{2}\.log(?:\.zip)?$",
    re.IGNORECASE,
)

COLUMNS: List[str] = [
    "source_file_name",
    "request_document_id",
    "calling_system_number",
    "host_system",
    "company_role",
    "currency_code",
    "invoice_header_date",
    "invoice_tax_determination_date",
    "invoice_transaction_type",
    "line_id",
    "commodity_code",
    "delivery_terms",
    "is_business_supply",
    "is_credit",
    "line_invoice_date",
    "line_number",
    "order_acceptance_country",
    "order_acceptance_state",
    "order_acceptance_postcode",
    "order_acceptance_geocode",
    "order_origin_country",
    "order_origin_state",
    "order_origin_postcode",
    "order_origin_geocode",
    "ship_from_country",
    "ship_from_state",
    "ship_from_postcode",
    "ship_from_geocode",
    "ship_from_location_tax_category",
    "invoice_number",
    "original_invoice_number",
    "invoice_date",
    "external_company_id",
    "customer_number",
    "gross_amount",
    "ship_to_country",
    "ship_to_state",
    "ship_to_postcode",
    "ship_to_geocode",
    "quantity",
    "unit_of_measure",
    "supplementary_unit",
    "part_number",
    "country_of_origin",
    "point_of_title_transfer",
    "title_transfer_location",
    "product_code",
    "regime",
    "line_tax_determination_date",
    "line_transaction_type",
    "inclusive_tax_indicators",
    "exempt_amount_country",
    "exempt_amount_province",
    "exempt_amount_state",
    "exempt_amount_county",
    "exempt_amount_city",
    "exempt_amount_district",
    "exempt_amount_postcode",
    "exempt_amount_geocode",
    "source_system",
    "attribute_1",
    "attribute_24",
    "attribute_48",
    "attribute_2",
    "attribute_5",
    "attribute_6",
    "attribute_9",
    "attribute_10",
    "attribute_14",
    "attribute_18",
    "attribute_19",
    "attribute_20",
    "attribute_22",
    "attribute_26",
    "attribute_32",
    "attribute_37",
    "attribute_39",
    "tax_summary_taxable_basis",
    "tax_summary_non_taxable_basis",
    "tax_summary_exempt_amount",
    "tax_summary_tax_rate",
    "tax_summary_effective_tax_rate",
    "tax_summary_advisory",
    "total_tax_amount",
]

REQUEST_DOC_START_RE = re.compile(
    r"Starting request processing for id\s+RFC_CALCULATE_TAXES_DOC(?P<id>[0-9Ee+\-.]+)",
    re.IGNORECASE,
)
DOC_BLOCK_START_RE = re.compile(r"\*?\s*DOC(?P<id>\d+)\]?\s+beginning", re.IGNORECASE)
DOC_BLOCK_END_RE = re.compile(r"RFC_CALCULATE_TAXES_DOC(?P<id>[0-9Ee+\-.]+)\b", re.IGNORECASE)


def parse_date(value: str) -> date:
    value = value.strip()
    for fmt in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    raise argparse.ArgumentTypeError(
        f"Invalid date '{value}'. Use YYYY-MM-DD or YYYYMMDD."
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert Sabrix logs into sample_production_data.csv format."
    )
    parser.add_argument(
        "--config",
        default=str(DEFAULT_CONFIG_FILE),
        help="Path to a JSON config file.",
    )
    parser.add_argument(
        "--input-dir",
        default=None,
        help="Directory containing SabrixIntegrationServer log/log.zip files.",
    )
    parser.add_argument(
        "--output-csv",
        default=None,
        help="Output CSV path. Defaults to output/sabrix_formatted_<timestamp>.csv",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Directory to write output CSV when --output-csv is not provided.",
    )
    parser.add_argument(
        "--date",
        type=parse_date,
        default=None,
        help="Only process files matching this date in filename (YYYY-MM-DD or YYYYMMDD).",
    )
    parser.add_argument(
        "--start-date",
        type=parse_date,
        default=None,
        help="Start date for filename filtering (inclusive).",
    )
    parser.add_argument(
        "--end-date",
        type=parse_date,
        default=None,
        help="End date for filename filtering (inclusive).",
    )
    parser.add_argument(
        "--recursive",
        action="store_true",
        default=None,
        help="Recursively scan subdirectories under --input-dir.",
    )
    parser.add_argument(
        "--no-recursive",
        action="store_false",
        dest="recursive",
        help="Only scan the top-level of --input-dir.",
    )
    return parser.parse_args()


def load_config(config_path: str) -> Dict[str, str]:
    path = Path(config_path)
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def parse_optional_date(value: Optional[str]) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, str) and not value.strip():
        return None
    if isinstance(value, date):
        return value
    return parse_date(str(value))


def normalize_request_document_id(value: str) -> str:
    """Convert request document id to a plain, readable string (no scientific notation)."""
    text = str(value).strip()
    if not text:
        return ""

    try:
        numeric = Decimal(text)
    except (InvalidOperation, ValueError):
        return text

    if numeric == numeric.to_integral_value():
        return str(numeric.quantize(Decimal("1")))

    plain = format(numeric.normalize(), 'f')
    return plain.rstrip('0').rstrip('.')


def resolve_runtime_settings(
    args: argparse.Namespace,
    config: Dict[str, str],
) -> Tuple[Path, Optional[date], Optional[date], Optional[date], Path, bool]:
    input_dir = Path(args.input_dir or config.get("input_dir") or str(DEFAULT_INPUT_DIR))

    configured_output_dir = Path(config.get("output_dir")) if config.get("output_dir") else DEFAULT_OUTPUT_DIR
    output_dir = Path(args.output_dir) if args.output_dir else configured_output_dir

    date_filter = args.date or parse_optional_date(config.get("date"))
    start_date = args.start_date or parse_optional_date(config.get("start_date"))
    end_date = args.end_date or parse_optional_date(config.get("end_date"))

    output_csv = (
        Path(args.output_csv)
        if args.output_csv
        else output_dir / f"sabrix_formatted_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    )

    if args.recursive is None:
        recursive = bool(config.get("recursive", True))
    else:
        recursive = bool(args.recursive)

    return input_dir, date_filter, start_date, end_date, output_csv, recursive


def get_file_date(path: Path) -> Optional[date]:
    match = FILE_DATE_RE.search(path.name)
    if not match:
        return None
    return datetime.strptime(match.group("date"), "%Y-%m-%d").date()


def should_include(file_date: date, exact: Optional[date], start: Optional[date], end: Optional[date]) -> bool:
    if exact is not None:
        return file_date == exact
    if start is not None and file_date < start:
        return False
    if end is not None and file_date > end:
        return False
    return True


def iter_log_files(
    input_dir: Path,
    exact: Optional[date],
    start: Optional[date],
    end: Optional[date],
    recursive: bool,
) -> Iterable[Path]:
    candidates: List[Tuple[date, Path]] = []
    iterator = input_dir.rglob("*") if recursive else input_dir.iterdir()
    for path in iterator:
        if not path.is_file():
            continue
        if not (path.name.lower().endswith(".log") or path.name.lower().endswith(".log.zip")):
            continue

        file_date = get_file_date(path)
        if file_date is None:
            continue
        if should_include(file_date, exact, start, end):
            candidates.append((file_date, path))

    for _, path in sorted(candidates, key=lambda item: (item[0], str(item[1]).lower())):
        yield path


def build_source_file_name(path: Path, input_dir: Path) -> str:
    """Use relative path so same file names in different server folders stay unique."""
    try:
        return str(path.relative_to(input_dir)).replace("\\", "/")
    except ValueError:
        return path.name


def read_log_text(path: Path) -> str:
    if path.suffix.lower() == ".zip":
        with zipfile.ZipFile(path, "r") as archive:
            entries = [entry for entry in archive.infolist() if not entry.is_dir()]
            if not entries:
                return ""
            # Prefer the first .log entry if present.
            log_entry = next((e for e in entries if e.filename.lower().endswith(".log")), entries[0])
            with archive.open(log_entry, "r") as stream:
                # Decode in chunks to avoid allocating a huge unzip buffer at once.
                decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
                parts: List[str] = []
                while True:
                    chunk = stream.read(1024 * 1024)
                    if not chunk:
                        break
                    parts.append(decoder.decode(chunk))
                parts.append(decoder.decode(b"", final=True))
                return "".join(parts)

    return path.read_text(encoding="utf-8", errors="replace")


def normalize_value(value: str) -> str:
    return " ".join(value.strip().split())


def get_tag_values(text: str, tag: str) -> List[str]:
    pattern = re.compile(rf"<{tag}(?:\s+[^>]*)?>(.*?)</{tag}>", re.DOTALL)
    return [normalize_value(match.group(1)) for match in pattern.finditer(text)]


def get_tag_blocks(text: str, tag: str) -> List[str]:
    pattern = re.compile(rf"<{tag}(?:\s+[^>]*)?>(.*?)</{tag}>", re.DOTALL)
    return [match.group(1) for match in pattern.finditer(text)]


def get_first_tag_value(text: str, tag: str) -> str:
    values = get_tag_values(text, tag)
    return values[0] if values else ""


def get_first_non_empty_tag_value(text: str, tags: Sequence[str]) -> str:
    for tag in tags:
        value = get_first_tag_value(text, tag)
        if value:
            return value
    return ""


def find_last_request_document_id_before(
    start_markers: Sequence[Tuple[int, str]],
    position: int,
) -> str:
    last_id = ""
    for marker_pos, marker_id in start_markers:
        if marker_pos > position:
            break
        last_id = marker_id
    return last_id


def extract_doc_request_blocks(text: str) -> List[Tuple[str, str]]:
    """Extract transaction blocks framed by DOC<id> beginning ... RFC_CALCULATE_TAXES_DOC<id>."""
    blocks: List[Tuple[str, str]] = []
    lines = text.splitlines(keepends=True)

    current_doc_id = ""
    current_lines: List[str] = []

    for line in lines:
        start_match = DOC_BLOCK_START_RE.search(line)
        if start_match:
            if current_doc_id and current_lines:
                # Keep incomplete prior block to avoid dropping data.
                blocks.append((current_doc_id, "".join(current_lines)))

            current_doc_id = normalize_request_document_id(start_match.group("id"))
            current_lines = [line]
            continue

        if not current_doc_id:
            continue

        current_lines.append(line)
        end_match = DOC_BLOCK_END_RE.search(line)
        if not end_match:
            continue

        end_doc_id = normalize_request_document_id(end_match.group("id"))
        if end_doc_id == current_doc_id:
            blocks.append((current_doc_id, "".join(current_lines)))
            current_doc_id = ""
            current_lines = []

    if current_doc_id and current_lines:
        blocks.append((current_doc_id, "".join(current_lines)))

    return blocks


def get_line_entries(invoice_block: str) -> List[Tuple[str, str]]:
    line_re = re.compile(r"<LINE(?:\s+([^>]*))?>(.*?)</LINE>", re.DOTALL)
    entries: List[Tuple[str, str]] = []
    for attrs, inner in line_re.findall(invoice_block):
        line_id = ""
        if attrs:
            id_match = re.search(r'\bID\s*=\s*"([^"]*)"', attrs, re.IGNORECASE)
            if id_match:
                line_id = normalize_value(id_match.group(1))
        entries.append((line_id, inner))
    return entries


def get_quantity_amounts_and_uoms(text: str) -> Tuple[List[str], List[str]]:
    amounts: List[str] = []
    uoms: List[str] = []
    quantity_blocks = get_tag_values(text, "QUANTITY")

    for block in quantity_blocks:
        amount_match = re.search(r"<AMOUNT>(.*?)</AMOUNT>", block, re.DOTALL)
        uom_match = re.search(r"<UOM>(.*?)</UOM>", block, re.DOTALL)

        if amount_match:
            amounts.append(normalize_value(amount_match.group(1)))
        else:
            amounts.append(normalize_value(re.sub(r"<[^>]+>", " ", block)))

        if uom_match:
            uoms.append(normalize_value(uom_match.group(1)))
        else:
            uoms.append("")

    return amounts, uoms


def get_quantity_from_line(line_block: str) -> Tuple[str, str]:
    quantity_blocks = get_tag_values(line_block, "QUANTITY")
    if not quantity_blocks:
        return "", get_first_tag_value(line_block, "UNIT_OF_MEASURE")

    block = quantity_blocks[0]
    amount_match = re.search(r"<AMOUNT>(.*?)</AMOUNT>", block, re.DOTALL)
    uom_match = re.search(r"<UOM>(.*?)</UOM>", block, re.DOTALL)

    amount = normalize_value(amount_match.group(1)) if amount_match else normalize_value(re.sub(r"<[^>]+>", " ", block))
    uom = normalize_value(uom_match.group(1)) if uom_match else get_first_non_empty_tag_value(
        line_block,
        ["UNIT_OF_MEASURE", "SUPPLEMENTARY_UNIT"],
    )
    return amount, uom


def get_user_elements(line_block: str) -> Dict[str, str]:
    pairs = re.findall(
        r"<USER_ELEMENT>\s*<NAME>(.*?)</NAME>\s*<VALUE>(.*?)</VALUE>\s*</USER_ELEMENT>",
        line_block,
        re.DOTALL,
    )
    values: Dict[str, str] = {}
    for name, value in pairs:
        key = normalize_value(name).upper()
        val = normalize_value(value)
        if key:
            values[key] = val
    return values


def normalize_external_company_id(value: str) -> str:
    clean = normalize_value(value)
    if "-" in clean:
        return clean.split("-")[-1]
    return clean


def get_ship_to_block(line_block: str) -> str:
    # Primary location source in the provided INDATA examples.
    ship_to_blocks = get_tag_blocks(line_block, "SHIP_TO")
    if ship_to_blocks:
        return ship_to_blocks[0]

    # Fallbacks when SHIP_TO is absent in certain payload variants.
    for tag in ("ORDER_ACCEPTANCE", "ORDER_ORIGIN"):
        blocks = get_tag_blocks(line_block, tag)
        if blocks:
            return blocks[0]

    return ""


def split_unique_invoice_number(value: str) -> Tuple[str, str, str]:
    parts = value.split("|")
    while len(parts) < 3:
        parts.append("")
    return parts[0].strip(), parts[1].strip(), parts[2].strip()


def build_outdata_lookup(text: str) -> Tuple[List[Dict[str, str]], Dict[str, List[int]], Dict[str, List[int]]]:
    """Build OUTDATA lookup records with ordered and keyed access."""
    records: List[Dict[str, str]] = []
    by_unique: Dict[str, List[int]] = {}
    by_invoice: Dict[str, List[int]] = {}

    outdata_blocks = re.findall(r"<OUTDATA(?:\s+[^>]*)?>.*?</OUTDATA>", text, re.DOTALL)
    for outdata in outdata_blocks:
        invoice_blocks = get_tag_blocks(outdata, "INVOICE")
        if not invoice_blocks:
            continue

        invoice_block = invoice_blocks[0]
        unique_invoice = get_first_tag_value(invoice_block, "UNIQUE_INVOICE_NUMBER")
        invoice_number = get_first_tag_value(invoice_block, "INVOICE_NUMBER")
        original_invoice_number = get_first_tag_value(invoice_block, "ORIGINAL_INVOICE_NUMBER")
        total_tax_amount = get_first_tag_value(invoice_block, "TOTAL_TAX_AMOUNT")
        line_tax_summaries = extract_tax_summary_per_line(invoice_block)

        record = {
            "unique_invoice_number": unique_invoice,
            "invoice_number": invoice_number,
            "original_invoice_number": original_invoice_number,
            "total_tax_amount": total_tax_amount,
            "line_tax_summaries": line_tax_summaries,
        }
        record_index = len(records)
        records.append(record)

        if unique_invoice:
            by_unique.setdefault(unique_invoice, []).append(record_index)
        if invoice_number:
            by_invoice.setdefault(invoice_number, []).append(record_index)

    return records, by_unique, by_invoice


def extract_tax_summary_per_line(invoice_block: str) -> List[Dict[str, str]]:
    """Extract line-level TAX_SUMMARY values from an OUTDATA INVOICE block."""
    line_blocks = get_tag_blocks(invoice_block, "LINE")
    summaries: List[Dict[str, str]] = []

    for line_block in line_blocks:
        summary = {
            "tax_summary_taxable_basis": "",
            "tax_summary_non_taxable_basis": "",
            "tax_summary_exempt_amount": "",
            "tax_summary_tax_rate": "",
            "tax_summary_effective_tax_rate": "",
            "tax_summary_advisory": "",
        }

        tax_summary_blocks = get_tag_blocks(line_block, "TAX_SUMMARY")
        if tax_summary_blocks:
            tax_summary = tax_summary_blocks[0]
            summary["tax_summary_taxable_basis"] = get_first_tag_value(tax_summary, "TAXABLE_BASIS")
            summary["tax_summary_non_taxable_basis"] = get_first_tag_value(tax_summary, "NON_TAXABLE_BASIS")
            summary["tax_summary_exempt_amount"] = get_first_tag_value(tax_summary, "EXEMPT_AMOUNT")
            summary["tax_summary_tax_rate"] = get_first_tag_value(tax_summary, "TAX_RATE")
            summary["tax_summary_effective_tax_rate"] = get_first_tag_value(tax_summary, "EFFECTIVE_TAX_RATE")

            advisories_block = get_tag_blocks(tax_summary, "ADVISORIES")
            if advisories_block:
                advisories = get_tag_values(advisories_block[0], "ADVISORY")
                summary["tax_summary_advisory"] = " | ".join([v for v in advisories if v])

        summaries.append(summary)

    return summaries


def pick_outdata_record(
    records: List[Dict[str, str]],
    by_unique: Dict[str, List[int]],
    by_invoice: Dict[str, List[int]],
    used_indices: Set[int],
    sequence_index: int,
    unique_invoice: str,
    invoice_number: str,
) -> Optional[Dict[str, str]]:
    """Pick best matching OUTDATA record for an INDATA row."""

    def first_unused(indices: List[int]) -> Optional[int]:
        for idx in indices:
            if idx not in used_indices:
                return idx
        return None

    if unique_invoice and unique_invoice in by_unique:
        idx = first_unused(by_unique[unique_invoice])
        if idx is not None:
            used_indices.add(idx)
            return records[idx]

    if invoice_number and invoice_number in by_invoice:
        idx = first_unused(by_invoice[invoice_number])
        if idx is not None:
            used_indices.add(idx)
            return records[idx]

    if 0 <= sequence_index < len(records) and sequence_index not in used_indices:
        used_indices.add(sequence_index)
        return records[sequence_index]

    return None


def extract_rows_from_indata(
    text: str,
    forced_request_document_id: Optional[str] = None,
) -> List[Dict[str, str]]:
    indata_matches = list(re.finditer(r"<INDATA(?:\s+[^>]*)?>.*?</INDATA>", text, re.DOTALL))
    if not indata_matches:
        return []

    indata_blocks = [m.group(0) for m in indata_matches]
    outdata_records, outdata_by_unique, outdata_by_invoice = build_outdata_lookup(text)
    used_outdata_indices: Set[int] = set()
    request_markers = []
    if forced_request_document_id is None:
        request_markers = [
            (m.start(), normalize_request_document_id(m.group("id")))
            for m in REQUEST_DOC_START_RE.finditer(text)
        ]

    rows: List[Dict[str, str]] = []

    for indata_index, indata in enumerate(indata_blocks):
        if forced_request_document_id is not None:
            request_document_id = forced_request_document_id
        else:
            request_document_id = find_last_request_document_id_before(
                request_markers,
                indata_matches[indata_index].start(),
            )

        invoice_blocks = get_tag_blocks(indata, "INVOICE")
        invoice_block = invoice_blocks[0] if invoice_blocks else indata

        calling_system_number = get_first_tag_value(indata, "CALLING_SYSTEM_NUMBER")
        host_system = get_first_tag_value(indata, "HOST_SYSTEM")
        company_role = get_first_tag_value(invoice_block, "COMPANY_ROLE")
        currency_code = get_first_tag_value(invoice_block, "CURRENCY_CODE")

        invoice_header_date = get_first_non_empty_tag_value(
            invoice_block,
            ["INVOICE_DATE", "TAX_DETERMINATION_DATE"],
        )
        invoice_tax_determination_date = get_first_tag_value(invoice_block, "TAX_DETERMINATION_DATE")
        invoice_transaction_type = get_first_tag_value(invoice_block, "TRANSACTION_TYPE")

        invoice_date = invoice_header_date
        invoice_number = get_first_tag_value(invoice_block, "INVOICE_NUMBER")
        external_company_id = normalize_external_company_id(get_first_tag_value(invoice_block, "EXTERNAL_COMPANY_ID"))

        unique_invoice = get_first_tag_value(invoice_block, "UNIQUE_INVOICE_NUMBER")
        if unique_invoice:
            unique_external, unique_invoice_number, _ = split_unique_invoice_number(unique_invoice)
            if not invoice_number:
                invoice_number = unique_invoice_number
            if not external_company_id:
                external_company_id = normalize_external_company_id(unique_external)

        original_invoice_number = get_first_tag_value(invoice_block, "ORIGINAL_INVOICE_NUMBER")
        total_tax_amount = get_first_tag_value(invoice_block, "TOTAL_TAX_AMOUNT")

        outdata_record = pick_outdata_record(
            outdata_records,
            outdata_by_unique,
            outdata_by_invoice,
            used_outdata_indices,
            indata_index,
            unique_invoice,
            invoice_number,
        )

        if outdata_record:
            if not original_invoice_number:
                original_invoice_number = outdata_record.get("original_invoice_number", "")
            if not total_tax_amount:
                total_tax_amount = outdata_record.get("total_tax_amount", "")

        line_entries = get_line_entries(invoice_block)
        if not line_entries:
            line_entries = [("", invoice_block)]

        for line_idx, (line_id, line_block) in enumerate(line_entries):
            ship_to_block = get_ship_to_block(line_block)
            order_acceptance_block = get_tag_blocks(line_block, "ORDER_ACCEPTANCE")
            order_origin_block = get_tag_blocks(line_block, "ORDER_ORIGIN")
            ship_from_block = get_tag_blocks(line_block, "SHIP_FROM")
            exempt_amount_block = get_tag_blocks(line_block, "EXEMPT_AMOUNT")

            order_acceptance = order_acceptance_block[0] if order_acceptance_block else ""
            order_origin = order_origin_block[0] if order_origin_block else ""
            ship_from = ship_from_block[0] if ship_from_block else ""
            exempt_amount = exempt_amount_block[0] if exempt_amount_block else ""

            quantity, quantity_uom = get_quantity_from_line(line_block)
            unit_of_measure = get_first_non_empty_tag_value(
                line_block,
                ["UNIT_OF_MEASURE", "SUPPLEMENTARY_UNIT"],
            ) or quantity_uom
            supplementary_unit = get_first_tag_value(line_block, "SUPPLEMENTARY_UNIT")
            user_elements = get_user_elements(line_block)

            line_invoice_date = get_first_non_empty_tag_value(
                line_block,
                ["INVOICE_DATE", "TAX_DETERMINATION_DATE"],
            )
            line_tax_determination_date = get_first_tag_value(line_block, "TAX_DETERMINATION_DATE")
            line_transaction_type = get_first_tag_value(line_block, "TRANSACTION_TYPE")

            source_system = get_first_non_empty_tag_value(
                line_block,
                ["SOURCE_SYSTEM"],
            ) or get_first_non_empty_tag_value(invoice_block, ["SOURCE_SYSTEM"]) 

            if not source_system and host_system:
                source_system = host_system

            tax_summary_values = {
                "tax_summary_taxable_basis": "",
                "tax_summary_non_taxable_basis": "",
                "tax_summary_exempt_amount": "",
                "tax_summary_tax_rate": "",
                "tax_summary_effective_tax_rate": "",
                "tax_summary_advisory": "",
            }
            if outdata_record:
                summaries = outdata_record.get("line_tax_summaries", [])
                if isinstance(summaries, list) and summaries:
                    if line_idx < len(summaries):
                        tax_summary_values = summaries[line_idx]
                    else:
                        tax_summary_values = summaries[0]

            row = {
                "request_document_id": request_document_id,
                "calling_system_number": calling_system_number,
                "host_system": host_system,
                "company_role": company_role,
                "currency_code": currency_code,
                "invoice_header_date": invoice_header_date,
                "invoice_tax_determination_date": invoice_tax_determination_date,
                "invoice_transaction_type": invoice_transaction_type,
                "line_id": line_id,
                "commodity_code": get_first_tag_value(line_block, "COMMODITY_CODE"),
                "delivery_terms": get_first_tag_value(line_block, "DELIVERY_TERMS"),
                "is_business_supply": get_first_tag_value(line_block, "IS_BUSINESS_SUPPLY"),
                "is_credit": get_first_tag_value(line_block, "IS_CREDIT"),
                "line_invoice_date": line_invoice_date,
                "line_number": get_first_tag_value(line_block, "LINE_NUMBER"),
                "order_acceptance_country": get_first_tag_value(order_acceptance, "COUNTRY"),
                "order_acceptance_state": get_first_tag_value(order_acceptance, "STATE"),
                "order_acceptance_postcode": get_first_tag_value(order_acceptance, "POSTCODE"),
                "order_acceptance_geocode": get_first_tag_value(order_acceptance, "GEOCODE"),
                "order_origin_country": get_first_tag_value(order_origin, "COUNTRY"),
                "order_origin_state": get_first_tag_value(order_origin, "STATE"),
                "order_origin_postcode": get_first_tag_value(order_origin, "POSTCODE"),
                "order_origin_geocode": get_first_tag_value(order_origin, "GEOCODE"),
                "ship_from_country": get_first_tag_value(ship_from, "COUNTRY"),
                "ship_from_state": get_first_tag_value(ship_from, "STATE"),
                "ship_from_postcode": get_first_tag_value(ship_from, "POSTCODE"),
                "ship_from_geocode": get_first_tag_value(ship_from, "GEOCODE"),
                "ship_from_location_tax_category": get_first_tag_value(ship_from, "LOCATION_TAX_CATEGORY"),
                "invoice_number": invoice_number,
                "original_invoice_number": original_invoice_number,
                "invoice_date": line_invoice_date or invoice_date,
                "external_company_id": external_company_id,
                "customer_number": get_first_tag_value(line_block, "CUSTOMER_NUMBER"),
                "gross_amount": get_first_tag_value(line_block, "GROSS_AMOUNT"),
                "ship_to_country": get_first_tag_value(ship_to_block, "COUNTRY"),
                "ship_to_state": get_first_tag_value(ship_to_block, "STATE"),
                "ship_to_postcode": get_first_tag_value(ship_to_block, "POSTCODE"),
                "ship_to_geocode": get_first_tag_value(ship_to_block, "GEOCODE"),
                "quantity": quantity,
                "unit_of_measure": unit_of_measure,
                "supplementary_unit": supplementary_unit,
                "part_number": get_first_tag_value(line_block, "PART_NUMBER"),
                "country_of_origin": get_first_tag_value(line_block, "COUNTRY_OF_ORIGIN"),
                "point_of_title_transfer": get_first_tag_value(line_block, "POINT_OF_TITLE_TRANSFER"),
                "title_transfer_location": get_first_tag_value(line_block, "TITLE_TRANSFER_LOCATION"),
                "product_code": get_first_tag_value(line_block, "PRODUCT_CODE"),
                "regime": get_first_tag_value(line_block, "REGIME"),
                "line_tax_determination_date": line_tax_determination_date,
                "line_transaction_type": line_transaction_type,
                "inclusive_tax_indicators": get_first_tag_value(line_block, "INCLUSIVE_TAX_INDICATORS"),
                "exempt_amount_country": get_first_tag_value(exempt_amount, "COUNTRY"),
                "exempt_amount_province": get_first_tag_value(exempt_amount, "PROVINCE"),
                "exempt_amount_state": get_first_tag_value(exempt_amount, "STATE"),
                "exempt_amount_county": get_first_tag_value(exempt_amount, "COUNTY"),
                "exempt_amount_city": get_first_tag_value(exempt_amount, "CITY"),
                "exempt_amount_district": get_first_tag_value(exempt_amount, "DISTRICT"),
                "exempt_amount_postcode": get_first_tag_value(exempt_amount, "POSTCODE"),
                "exempt_amount_geocode": get_first_tag_value(exempt_amount, "GEOCODE"),
                "source_system": source_system,
                "attribute_1": user_elements.get("ATTRIBUTE1", get_first_tag_value(line_block, "ATTRIBUTE1")),
                "attribute_24": user_elements.get("ATTRIBUTE24", get_first_tag_value(line_block, "ATTRIBUTE24")),
                "attribute_48": user_elements.get("ATTRIBUTE48", get_first_tag_value(line_block, "ATTRIBUTE48")),
                "attribute_2": user_elements.get("ATTRIBUTE2", get_first_tag_value(line_block, "ATTRIBUTE2")),
                "attribute_5": user_elements.get("ATTRIBUTE5", get_first_tag_value(line_block, "ATTRIBUTE5")),
                "attribute_6": user_elements.get("ATTRIBUTE6", get_first_tag_value(line_block, "ATTRIBUTE6")),
                "attribute_9": user_elements.get("ATTRIBUTE9", get_first_tag_value(line_block, "ATTRIBUTE9")),
                "attribute_10": user_elements.get("ATTRIBUTE10", get_first_tag_value(line_block, "ATTRIBUTE10")),
                "attribute_14": user_elements.get("ATTRIBUTE14", get_first_tag_value(line_block, "ATTRIBUTE14")),
                "attribute_18": user_elements.get("ATTRIBUTE18", get_first_tag_value(line_block, "ATTRIBUTE18")),
                "attribute_19": user_elements.get("ATTRIBUTE19", get_first_tag_value(line_block, "ATTRIBUTE19")),
                "attribute_20": user_elements.get("ATTRIBUTE20", get_first_tag_value(line_block, "ATTRIBUTE20")),
                "attribute_22": user_elements.get("ATTRIBUTE22", get_first_tag_value(line_block, "ATTRIBUTE22")),
                "attribute_26": user_elements.get("ATTRIBUTE26", get_first_tag_value(line_block, "ATTRIBUTE26")),
                "attribute_32": user_elements.get("ATTRIBUTE32", get_first_tag_value(line_block, "ATTRIBUTE32")),
                "attribute_37": user_elements.get("ATTRIBUTE37", get_first_tag_value(line_block, "ATTRIBUTE37")),
                "attribute_39": user_elements.get("ATTRIBUTE39", get_first_tag_value(line_block, "ATTRIBUTE39")),
                "tax_summary_taxable_basis": tax_summary_values.get("tax_summary_taxable_basis", ""),
                "tax_summary_non_taxable_basis": tax_summary_values.get("tax_summary_non_taxable_basis", ""),
                "tax_summary_exempt_amount": tax_summary_values.get("tax_summary_exempt_amount", ""),
                "tax_summary_tax_rate": tax_summary_values.get("tax_summary_tax_rate", ""),
                "tax_summary_effective_tax_rate": tax_summary_values.get("tax_summary_effective_tax_rate", ""),
                "tax_summary_advisory": tax_summary_values.get("tax_summary_advisory", ""),
                "total_tax_amount": total_tax_amount,
            }
            rows.append(row)

    return rows


def last_tag_before(text: str, tag: str, position: int, search_window: int = 50000) -> str:
    start = max(0, position - search_window)
    window = text[start:position]
    values = get_tag_values(window, tag)
    return values[-1] if values else ""


def choose(values: Sequence[str], index: int) -> str:
    if not values:
        return ""
    if index < len(values):
        return values[index]
    return values[-1]


def extract_rows_from_legacy_unique_invoice(text: str) -> List[Dict[str, str]]:
    unique_re = re.compile(r"<UNIQUE_INVOICE_NUMBER>(.*?)</UNIQUE_INVOICE_NUMBER>", re.DOTALL)
    matches = list(unique_re.finditer(text))
    if not matches:
        return []

    rows: List[Dict[str, str]] = []

    for i, match in enumerate(matches):
        unique_invoice = normalize_value(match.group(1))
        external_company_id, invoice_number, _company_role = split_unique_invoice_number(unique_invoice)

        block_start = match.end()
        block_end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        block = text[block_start:block_end]

        transaction_date = last_tag_before(text, "TRANSACTION_DATE", match.start())
        invoice_date = transaction_date.replace("-", "") if transaction_date else ""

        quantity_amounts, quantity_uoms = get_quantity_amounts_and_uoms(block)

        field_values = {
            "customer_number": get_tag_values(block, "CUSTOMER_NUMBER"),
            "gross_amount": get_tag_values(block, "GROSS_AMOUNT"),
            "ship_to_country": get_tag_values(block, "SHIP_TO_COUNTRY"),
            "ship_to_state": get_tag_values(block, "SHIP_TO_STATE"),
            "ship_to_postcode": get_tag_values(block, "SHIP_TO_POSTCODE"),
            "ship_to_geocode": get_tag_values(block, "SHIP_TO_GEOCODE"),
            "quantity": quantity_amounts,
            "unit_of_measure": get_tag_values(block, "UNIT_OF_MEASURE"),
            "part_number": get_tag_values(block, "PART_NUMBER"),
            "country_of_origin": get_tag_values(block, "COUNTRY_OF_ORIGIN"),
            "source_system": get_tag_values(block, "SOURCE_SYSTEM"),
            "attribute_1": get_tag_values(block, "ATTRIBUTE1"),
            "attribute_24": get_tag_values(block, "ATTRIBUTE24"),
            "attribute_48": get_tag_values(block, "ATTRIBUTE48"),
            "attribute_2": get_tag_values(block, "ATTRIBUTE2"),
            "attribute_5": get_tag_values(block, "ATTRIBUTE5"),
            "attribute_6": get_tag_values(block, "ATTRIBUTE6"),
        }

        line_count = max(1, *(len(values) for values in field_values.values()))

        for idx in range(line_count):
            row = {
                "request_document_id": "",
                "calling_system_number": "",
                "host_system": "",
                "company_role": "",
                "currency_code": "",
                "invoice_header_date": invoice_date,
                "invoice_tax_determination_date": "",
                "invoice_transaction_type": "",
                "line_id": "",
                "commodity_code": "",
                "delivery_terms": "",
                "is_business_supply": "",
                "is_credit": "",
                "line_invoice_date": invoice_date,
                "line_number": "",
                "order_acceptance_country": "",
                "order_acceptance_state": "",
                "order_acceptance_postcode": "",
                "order_acceptance_geocode": "",
                "order_origin_country": "",
                "order_origin_state": "",
                "order_origin_postcode": "",
                "order_origin_geocode": "",
                "ship_from_country": "",
                "ship_from_state": "",
                "ship_from_postcode": "",
                "ship_from_geocode": "",
                "ship_from_location_tax_category": "",
                "invoice_number": invoice_number,
                "original_invoice_number": get_first_tag_value(block, "ORIGINAL_INVOICE_NUMBER"),
                "invoice_date": invoice_date,
                "external_company_id": external_company_id,
                "supplementary_unit": "",
                "point_of_title_transfer": "",
                "title_transfer_location": "",
                "product_code": "",
                "regime": "",
                "line_tax_determination_date": "",
                "line_transaction_type": "",
                "inclusive_tax_indicators": "",
                "exempt_amount_country": "",
                "exempt_amount_province": "",
                "exempt_amount_state": "",
                "exempt_amount_county": "",
                "exempt_amount_city": "",
                "exempt_amount_district": "",
                "exempt_amount_postcode": "",
                "exempt_amount_geocode": "",
            }
            for column in COLUMNS:
                if column in row:
                    continue
                row[column] = choose(field_values.get(column, []), idx)

            if not row["unit_of_measure"]:
                row["unit_of_measure"] = choose(quantity_uoms, idx)

            if not row.get("total_tax_amount"):
                row["total_tax_amount"] = get_first_tag_value(block, "TOTAL_TAX_AMOUNT")

            rows.append(row)

    return rows


def extract_rows_from_text(text: str) -> List[Dict[str, str]]:
    doc_blocks = extract_doc_request_blocks(text)
    if doc_blocks:
        scoped_rows: List[Dict[str, str]] = []
        for request_document_id, block_text in doc_blocks:
            scoped_rows.extend(
                extract_rows_from_indata(
                    block_text,
                    forced_request_document_id=request_document_id,
                )
            )
        if scoped_rows:
            return scoped_rows

    rows = extract_rows_from_indata(text)
    if rows:
        return rows
    return extract_rows_from_legacy_unique_invoice(text)


def assign_missing_original_invoice_numbers(rows: Sequence[Dict[str, str]]) -> None:
    """Assign incrementing unique ORIGINAL_INVOICE_NUMBER values where missing."""
    seed = 1
    used = {str(row.get("original_invoice_number", "")).strip() for row in rows if str(row.get("original_invoice_number", "")).strip()}

    for row in rows:
        current = str(row.get("original_invoice_number", "")).strip()
        if current:
            continue

        generated = f"AUTO_ORIG_{seed:08d}"
        while generated in used:
            seed += 1
            generated = f"AUTO_ORIG_{seed:08d}"

        row["original_invoice_number"] = generated
        used.add(generated)
        seed += 1


def write_csv(rows: Sequence[Dict[str, str]], output_csv: Path) -> None:
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with output_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in COLUMNS})


def main() -> int:
    args = parse_args()
    config = load_config(args.config)

    input_dir, date_filter, start_date, end_date, output_csv, recursive = resolve_runtime_settings(args, config)

    if not input_dir.exists() or not input_dir.is_dir():
        print(f"Input directory not found: {input_dir}")
        return 1

    if date_filter and (start_date or end_date):
        print("Use either --date or --start-date/--end-date, not both.")
        return 1

    all_rows: List[Dict[str, str]] = []
    processed_files = 0

    for path in iter_log_files(input_dir, date_filter, start_date, end_date, recursive):
        processed_files += 1
        try:
            text = read_log_text(path)
        except MemoryError:
            print(f"Skipping file due to memory limits while reading: {path}")
            continue
        rows = extract_rows_from_text(text)
        source_file_name = build_source_file_name(path, input_dir)
        for row in rows:
            row["source_file_name"] = source_file_name
        all_rows.extend(rows)

    assign_missing_original_invoice_numbers(all_rows)

    write_csv(all_rows, output_csv)

    print(f"Processed files: {processed_files}")
    print(f"Output rows: {len(all_rows)}")
    print(f"Output CSV: {output_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
