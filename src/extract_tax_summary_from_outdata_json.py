#!/usr/bin/env python3
"""Extract TAX_SUMMARY from outdata_mapped_json (or autdata_mapped_json) into a flat CSV."""

import argparse
import csv
import json
from pathlib import Path

DEFAULT_INPUT_CSV = Path("output/soap_response_results_06172026.csv")
DEFAULT_OUTPUT_CSV = Path("output/tax_summary_extracted_06172026_full.csv")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract TAX_SUMMARY from SOAP response JSON payload column into CSV."
    )
    parser.add_argument(
        "--input-csv",
        default=str(DEFAULT_INPUT_CSV),
        help="Path to input CSV that contains outdata_mapped_json/autdata_mapped_json column.",
    )
    parser.add_argument(
        "--output-csv",
        default=str(DEFAULT_OUTPUT_CSV),
        help="Path to output CSV.",
    )
    return parser.parse_args()


def get_payload(row: dict) -> str:
    for key in (
        "outdata_mapped_json",
        "OUTDATA_MAPPED_JSON",
        "autdata_mapped_json",
        "AUTDATA_MAPPED_JSON",
    ):
        value = row.get(key, "")
        if isinstance(value, str) and value.strip():
            return value
    return ""


def advisory_to_text(advisories) -> str:
    if not isinstance(advisories, dict):
        return ""
    advisory = advisories.get("ADVISORY", "")
    if isinstance(advisory, list):
        return " | ".join(str(x) for x in advisory)
    return str(advisory)


def strip_leading_zeros(value: str) -> str:
    text = str(value).strip()
    if not text:
        return ""
    if text.isdigit():
        return text.lstrip("0") or "0"
    return text


def user_element_value(user_elements, target_name: str) -> str:
    if not isinstance(user_elements, list):
        return ""
    for element in user_elements:
        if not isinstance(element, dict):
            continue
        name = str(element.get("NAME", "")).strip().upper()
        if name == target_name:
            return str(element.get("VALUE", "")).strip()
    return ""


def extract_material(item: dict) -> tuple[str, str]:
    part_number = str(item.get("PART_NUMBER", "")).strip()
    product_code = str(item.get("PRODUCT_CODE", "")).strip()
    commodity_code = str(item.get("COMMODITY_CODE", "")).strip()
    attribute32 = user_element_value(item.get("USER_ELEMENT"), "ATTRIBUTE32")
    attribute40 = user_element_value(item.get("USER_ELEMENT"), "ATTRIBUTE40")

    if part_number:
        return strip_leading_zeros(part_number), "PART_NUMBER"
    if product_code:
        return product_code, "PRODUCT_CODE"
    if commodity_code:
        return commodity_code, "COMMODITY_CODE"
    if attribute32:
        return attribute32, "USER_ELEMENT.ATTRIBUTE32"
    if attribute40:
        return attribute40, "USER_ELEMENT.ATTRIBUTE40"
    return "", ""


def main() -> int:
    args = parse_args()
    input_path = Path(args.input_csv)
    output_path = Path(args.output_csv)

    if not input_path.exists():
        print(f"Input file not found: {input_path}")
        return 1

    rows_out = []

    with input_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            payload = get_payload(row)
            if not payload:
                continue

            try:
                data = json.loads(payload)
            except json.JSONDecodeError:
                continue

            invoice = ((data.get("OUTDATA") or {}).get("INVOICE") or {})
            original_invoice_number = str(invoice.get("ORIGINAL_INVOICE_NUMBER", "")).strip()
            line = invoice.get("LINE")
            if line is None:
                continue

            line_items = line if isinstance(line, list) else [line]
            for line_index, item in enumerate(line_items, start=1):
                if not isinstance(item, dict):
                    continue

                tax_summary = item.get("TAX_SUMMARY")
                if not isinstance(tax_summary, dict):
                    continue

                material, material_source = extract_material(item)

                rows_out.append(
                    {
                        "row_index": row.get("row_index", ""),
                        "invoice_number": row.get("invoice_number", ""),
                        "original_invoice_number": original_invoice_number,
                        "line_index": line_index,
                        "material": material,
                        "material_source": material_source,
                        "taxable_basis": tax_summary.get("TAXABLE_BASIS", ""),
                        "non_taxable_basis": tax_summary.get("NON_TAXABLE_BASIS", ""),
                        "exempt_amount": tax_summary.get("EXEMPT_AMOUNT", ""),
                        "tax_rate": tax_summary.get("TAX_RATE", ""),
                        "effective_tax_rate": tax_summary.get("EFFECTIVE_TAX_RATE", ""),
                        "advisory": advisory_to_text(tax_summary.get("ADVISORIES")),
                    }
                )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "row_index",
                "invoice_number",
                "original_invoice_number",
                "line_index",
                "material",
                "material_source",
                "taxable_basis",
                "non_taxable_basis",
                "exempt_amount",
                "tax_rate",
                "effective_tax_rate",
                "advisory",
            ],
        )
        writer.writeheader()
        writer.writerows(rows_out)

    print(f"Input CSV: {input_path}")
    print(f"Output CSV: {output_path}")
    print(f"TAX_SUMMARY rows extracted: {len(rows_out)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
