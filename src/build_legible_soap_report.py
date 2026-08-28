#!/usr/bin/env python3
"""Build a readable, linked Excel report for SOAP tax processing outputs."""

import argparse
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Iterable

import pandas as pd


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent

# Edit these paths directly in the IDE when you want to switch files.
DEFAULT_INPUT_CSV = PROJECT_ROOT / "input" / "sample_qa1.csv"
DEFAULT_STATUS_CSV = PROJECT_ROOT / "output" / "soap_response_results_qa1.csv"
DEFAULT_TAX_SUMMARY_CSV = PROJECT_ROOT / "output" / "tax_summary_extracted_06172026_full.csv"
DEFAULT_OUTPUT_XLSX = PROJECT_ROOT / "output" / "soap_legible_report.xlsx"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a legible Excel report linking input, SOAP status, and tax summary data."
    )
    parser.add_argument("--input-csv", default=str(DEFAULT_INPUT_CSV), help="Path to source input CSV.")
    parser.add_argument("--status-csv", default=str(DEFAULT_STATUS_CSV), help="Path to SOAP status CSV.")
    parser.add_argument(
        "--tax-summary-csv",
        default=str(DEFAULT_TAX_SUMMARY_CSV),
        help="Path to extracted tax summary CSV (optional).",
    )
    parser.add_argument("--output-xlsx", default=str(DEFAULT_OUTPUT_XLSX), help="Output Excel workbook path.")
    return parser.parse_args()


def normalize_id(value: object) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return ""

    try:
        numeric = Decimal(text)
    except (InvalidOperation, ValueError):
        return text

    if numeric == numeric.to_integral_value():
        return str(numeric.quantize(Decimal("1")))

    plain = format(numeric.normalize(), "f")
    return plain.rstrip("0").rstrip(".")


def first_present(row: pd.Series, candidates: Iterable[str]) -> str:
    for name in candidates:
        if name in row.index:
            value = str(row.get(name, "")).strip()
            if value and value.lower() != "nan":
                return value
    return ""


def add_link_keys_input(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    out["invoice_number_norm"] = out.apply(
        lambda r: normalize_id(first_present(r, ("invoice_number", "INVOICE_NUMBER"))), axis=1
    )
    out["original_invoice_number_norm"] = out.apply(
        lambda r: first_present(r, ("original_invoice_number", "ORIGINAL_INVOICE_NUMBER")), axis=1
    )
    out["line_number_norm"] = out.apply(
        lambda r: normalize_id(first_present(r, ("line_number", "LINE_NUMBER", "line_id", "LINE_ID"))), axis=1
    )

    out["doc_key"] = out["invoice_number_norm"]
    out["line_key"] = out.apply(
        lambda r: f"{r['invoice_number_norm']}|{r['original_invoice_number_norm']}|{r['line_number_norm']}",
        axis=1,
    )
    return out


def add_link_keys_status(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "invoice_number" not in out.columns:
        out["invoice_number"] = ""
    out["invoice_number_norm"] = out["invoice_number"].apply(normalize_id)
    out["doc_key"] = out["invoice_number_norm"]
    return out


def add_link_keys_tax(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "invoice_number" not in out.columns:
        out["invoice_number"] = ""
    if "original_invoice_number" not in out.columns:
        out["original_invoice_number"] = ""
    if "line_index" not in out.columns:
        out["line_index"] = ""

    out["invoice_number_norm"] = out["invoice_number"].apply(normalize_id)
    out["original_invoice_number_norm"] = out["original_invoice_number"].astype(str).str.strip()
    out["line_index_norm"] = out["line_index"].apply(normalize_id)
    out["doc_key"] = out["invoice_number_norm"]
    out["line_key"] = out.apply(
        lambda r: f"{r['invoice_number_norm']}|{r['original_invoice_number_norm']}|{r['line_index_norm']}",
        axis=1,
    )
    return out


def safe_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(0)


def build_invoice_overview(input_df: pd.DataFrame, status_df: pd.DataFrame, tax_df: pd.DataFrame) -> pd.DataFrame:
    base = (
        input_df.groupby("doc_key", as_index=False)
        .agg(
            line_count=("line_key", "count"),
            source_file_count=("source_file_name", lambda s: s.astype(str).nunique() if "source_file_name" in input_df.columns else 0),
            external_company_id=("external_company_id", "first"),
            company_role=("company_role", "first"),
            currency_code=("currency_code", "first"),
            gross_amount_sum=("gross_amount", lambda s: safe_numeric(s).sum() if "gross_amount" in input_df.columns else 0),
        )
        .rename(columns={"doc_key": "invoice_number"})
    )

    if not status_df.empty:
        status_keep = [
            c
            for c in ("doc_key", "status", "http_status", "is_success", "total_tax", "error_message")
            if c in status_df.columns
        ]
        status_view = status_df[status_keep].drop_duplicates(subset=["doc_key"]).rename(
            columns={"doc_key": "invoice_number"}
        )
        base = base.merge(status_view, on="invoice_number", how="left")

    if not tax_df.empty:
        tax_agg = (
            tax_df.groupby("doc_key", as_index=False)
            .agg(
                tax_lines=("line_index", "count"),
                taxable_basis_sum=("taxable_basis", lambda s: safe_numeric(s).sum()),
                non_taxable_basis_sum=("non_taxable_basis", lambda s: safe_numeric(s).sum()),
                exempt_amount_sum=("exempt_amount", lambda s: safe_numeric(s).sum()),
            )
            .rename(columns={"doc_key": "invoice_number"})
        )
        base = base.merge(tax_agg, on="invoice_number", how="left")

    return base.sort_values("invoice_number")


def fit_column_widths(writer: pd.ExcelWriter, frame: pd.DataFrame, sheet_name: str) -> None:
    worksheet = writer.sheets[sheet_name]
    worksheet.freeze_panes(1, 0)

    for idx, col in enumerate(frame.columns):
        max_len = max(
            len(str(col)),
            *(len(str(v)) for v in frame[col].head(1000).fillna("")),
        )
        worksheet.set_column(idx, idx, min(max(max_len + 2, 12), 48))


def main() -> int:
    args = parse_args()

    input_path = Path(args.input_csv)
    status_path = Path(args.status_csv)
    tax_path = Path(args.tax_summary_csv)
    output_path = Path(args.output_xlsx)

    if not input_path.exists():
        print(f"Input CSV not found: {input_path}")
        return 1
    if not status_path.exists():
        print(f"Status CSV not found: {status_path}")
        return 1

    input_df = pd.read_csv(input_path, dtype=str, keep_default_na=False)
    status_df = pd.read_csv(status_path, dtype=str, keep_default_na=False)
    tax_df = pd.read_csv(tax_path, dtype=str, keep_default_na=False) if tax_path.exists() else pd.DataFrame()

    input_df = add_link_keys_input(input_df)
    status_df = add_link_keys_status(status_df)
    tax_df = add_link_keys_tax(tax_df) if not tax_df.empty else tax_df

    overview_df = build_invoice_overview(input_df, status_df, tax_df)

    failed_df = pd.DataFrame()
    if "status" in overview_df.columns:
        failed_df = overview_df[overview_df["status"].astype(str).str.lower() != "success"].copy()

    # Keep high-value columns first for readability.
    input_priority = [
        "doc_key",
        "line_key",
        "source_file_name",
        "invoice_number",
        "original_invoice_number",
        "line_number",
        "external_company_id",
        "company_role",
        "currency_code",
        "gross_amount",
        "part_number",
        "customer_number",
        "ship_to_country",
        "ship_to_state",
        "ship_to_postcode",
    ]
    input_cols = [c for c in input_priority if c in input_df.columns] + [
        c for c in input_df.columns if c not in input_priority
    ]

    tax_priority = [
        "doc_key",
        "line_key",
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
    ]
    tax_cols = [c for c in tax_priority if c in tax_df.columns] + [c for c in tax_df.columns if c not in tax_priority]

    readme_df = pd.DataFrame(
        {
            "section": [
                "How records are linked",
                "doc_key",
                "line_key",
                "Recommended review order",
                "Step 1",
                "Step 2",
                "Step 3",
            ],
            "details": [
                "This workbook links input lines, SOAP status, and extracted tax summaries.",
                "Normalized invoice number used for invoice-level joins.",
                "invoice_number|original_invoice_number|line_number (or line_index) for line-level tracing.",
                "Start with Invoice_Overview, then Failed_Invoices, then drill into Input_Lines and Tax_Summary_Lines.",
                "Validate status/http_status/is_success in Invoice_Overview.",
                "Use Input_Lines to inspect source values sent to SOAP.",
                "Use Tax_Summary_Lines to inspect taxable/exempt/rate/advisory details.",
            ],
        }
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
        readme_df.to_excel(writer, sheet_name="ReadMe", index=False)
        overview_df.to_excel(writer, sheet_name="Invoice_Overview", index=False)
        if not failed_df.empty:
            failed_df.to_excel(writer, sheet_name="Failed_Invoices", index=False)
        input_df[input_cols].to_excel(writer, sheet_name="Input_Lines", index=False)
        if not tax_df.empty:
            tax_df[tax_cols].to_excel(writer, sheet_name="Tax_Summary_Lines", index=False)

        fit_column_widths(writer, readme_df, "ReadMe")
        fit_column_widths(writer, overview_df, "Invoice_Overview")
        if not failed_df.empty:
            fit_column_widths(writer, failed_df, "Failed_Invoices")
        fit_column_widths(writer, input_df[input_cols], "Input_Lines")
        if not tax_df.empty:
            fit_column_widths(writer, tax_df[tax_cols], "Tax_Summary_Lines")

    print(f"Input CSV: {input_path}")
    print(f"Status CSV: {status_path}")
    print(f"Tax summary CSV: {tax_path if tax_path.exists() else 'not provided'}")
    print(f"Workbook created: {output_path}")
    print(f"Invoices in overview: {len(overview_df)}")
    print(f"Failed invoices: {len(failed_df)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
