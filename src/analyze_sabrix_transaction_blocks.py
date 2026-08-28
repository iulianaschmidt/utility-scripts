#!/usr/bin/env python3
"""
Analyze Sabrix log transaction blocks by DOC id and extract full block data.

A transaction block is detected from:
- begin marker: *DOC<id>] beginning
- end marker: line containing RFC_CALCULATE_TAXES_DOC<id>.

Outputs:
1) Summary CSV (one row per request document id with block coverage flags)
2) Detailed JSON (per transaction block with INDATA/OUTDATA/JCOCALL and raw block data)
3) Excel files:
    - One file with one row per INDATA block
    - One file with one row per OUTDATA block
    - One file with one row per JCO request line
    - One file with request-id level INDATA/OUTDATA/JCO presence flags
"""

import argparse
import csv
import json
import re
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from openpyxl import Workbook

DEFAULT_INPUT_DIR = Path(r"X:\bfp\TR_Ticket06182026")
DEFAULT_OUTPUT_DIR = Path("output")

START_RE = re.compile(r"\*?\s*DOC(?P<doc_id>\d+)\]?\s+beginning", re.IGNORECASE)
END_RE = re.compile(r"RFC_CALCULATE_TAXES_DOC(?P<doc_id>\d+)\.", re.IGNORECASE)
TIMESTAMP_RE = re.compile(r"^(?P<ts>\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2},\d{3})")

INDATA_RE = re.compile(r"<INDATA(?:\s+[^>]*)?>.*?</INDATA>", re.IGNORECASE | re.DOTALL)
OUTDATA_RE = re.compile(r"<OUTDATA(?:\s+[^>]*)?>.*?</OUTDATA>", re.IGNORECASE | re.DOTALL)
JCOCALL_RE = re.compile(r"JCO\s*CALL|JCOCALL", re.IGNORECASE)

ERROR_LINE_RE = re.compile(r"\[ERROR\]|\bERROR\b|\bEXCEPTION\b|\bFATAL\b|SOAP\s+FAULT|HTTP\s*500", re.IGNORECASE)
XML_MESSAGE_RE = re.compile(
    r"<MESSAGE>.*?<CODE>(?P<code>.*?)</CODE>.*?<MESSAGE_TEXT>(?P<text>.*?)</MESSAGE_TEXT>.*?</MESSAGE>",
    re.IGNORECASE | re.DOTALL,
)
IS_SUCCESS_FALSE_RE = re.compile(r"<IS_SUCCESS>\s*false\s*</IS_SUCCESS>", re.IGNORECASE)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract DOC transaction blocks and save full INDATA/OUTDATA/JCOCALL data."
    )
    parser.add_argument(
        "--input-dir",
        default=str(DEFAULT_INPUT_DIR),
        help="Directory containing .log.zip files.",
    )
    parser.add_argument(
        "--output-summary-csv",
        default=None,
        help="Path to request_document_id summary CSV output.",
    )
    parser.add_argument(
        "--output-details-json",
        default=None,
        help="Path to detailed per-block JSON output.",
    )
    parser.add_argument(
        "--max-error-lines",
        type=int,
        default=20,
        help="Maximum number of matched error lines to keep per block in details JSON.",
    )
    return parser.parse_args()


def iter_log_files(input_dir: Path) -> Iterable[Path]:
    for path in sorted(input_dir.rglob("*")):
        if not path.is_file():
            continue
        lower = path.name.lower()
        if lower.endswith(".log.zip"):
            yield path


def iter_zip_xml_payloads(path: Path) -> Iterable[Tuple[str, str]]:
    """Yield (entry_name, text) for XML-formatted files inside a .log.zip archive."""
    with zipfile.ZipFile(path, "r") as archive:
        entries = [entry for entry in archive.infolist() if not entry.is_dir()]
        if not entries:
            return

        xml_entries = [entry for entry in entries if entry.filename.lower().endswith(".xml")]
        candidate_entries = xml_entries if xml_entries else entries

        for entry in candidate_entries:
            with archive.open(entry, "r") as stream:
                text = stream.read().decode("utf-8", errors="replace")

            # Keep XML-formatted payloads only.
            if "<" not in text or ">" not in text:
                continue
            if not any(tag in text for tag in ("<INDATA", "<OUTDATA", "<INVOICE", "<MESSAGE", "<soap:")):
                continue

            yield entry.filename, text


def normalize_space(value: str) -> str:
    return " ".join(value.split())


def extract_timestamp(line: str) -> str:
    match = TIMESTAMP_RE.search(line)
    return match.group("ts") if match else ""


def extract_message_entries(text: str) -> List[Dict[str, str]]:
    entries: List[Dict[str, str]] = []
    for match in XML_MESSAGE_RE.finditer(text):
        code = normalize_space(match.group("code"))
        message_text = normalize_space(match.group("text"))
        entries.append({"code": code, "message_text": message_text})
    return entries


def build_block_record(
    file_name: str,
    doc_id: str,
    start_line_number: int,
    start_line_text: str,
    end_line_number: int,
    end_line_text: str,
    block_lines: List[str],
    max_error_lines: int,
) -> Dict[str, object]:
    block_text = "".join(block_lines)

    indata_raw_blocks = [m.group(0) for m in INDATA_RE.finditer(block_text)]
    outdata_raw_blocks = [m.group(0) for m in OUTDATA_RE.finditer(block_text)]
    indata_blocks = [normalize_space(block) for block in indata_raw_blocks]
    outdata_blocks = [normalize_space(block) for block in outdata_raw_blocks]

    jcocall_lines = [normalize_space(line) for line in block_lines if JCOCALL_RE.search(line)]

    error_lines = [normalize_space(line) for line in block_lines if ERROR_LINE_RE.search(line)]
    if IS_SUCCESS_FALSE_RE.search(block_text):
        error_lines.append("IS_SUCCESS is false in OUTDATA")

    xml_messages = extract_message_entries(block_text)

    error_codes = sorted({entry["code"] for entry in xml_messages if entry["code"]})
    error_texts = sorted({entry["message_text"] for entry in xml_messages if entry["message_text"]})

    has_all_blocks = bool(indata_blocks) and bool(outdata_blocks) and bool(jcocall_lines)
    missing_blocks = []
    if not jcocall_lines:
        missing_blocks.append("rfc_call")
    if not indata_blocks:
        missing_blocks.append("indata")
    if not outdata_blocks:
        missing_blocks.append("outdata")

    status = "COMPLETE" if has_all_blocks else "MISSING_BLOCKS"

    summary = {
        "source_file_name": file_name,
        "doc_id": doc_id,
        "start_line": start_line_number,
        "end_line": end_line_number,
        "start_timestamp": extract_timestamp(start_line_text),
        "end_timestamp": extract_timestamp(end_line_text),
        "has_indata": bool(indata_blocks),
        "has_outdata": bool(outdata_blocks),
        "has_jcocall": bool(jcocall_lines),
        "contains_all_blocks": has_all_blocks,
        "missing_blocks": " | ".join(missing_blocks),
        "status": status,
        "error_count": len(error_lines) + len(error_codes) + len(error_texts),
        "error_codes": " | ".join(error_codes),
        "error_messages": " | ".join(error_texts),
    }

    details = {
        **summary,
        "error_lines": error_lines[:max_error_lines],
        "xml_messages": xml_messages,
        "indata_blocks": indata_blocks,
        "outdata_blocks": outdata_blocks,
        "indata_raw_blocks": indata_raw_blocks,
        "outdata_raw_blocks": outdata_raw_blocks,
        "jcocall_lines": jcocall_lines,
        "raw_block": block_text,
    }

    return {"summary": summary, "details": details}


def extract_blocks_from_text(text: str, file_name: str, max_error_lines: int) -> List[Dict[str, object]]:
    records: List[Dict[str, object]] = []
    lines = text.splitlines(keepends=True)

    current_doc_id: Optional[str] = None
    current_start_line: int = -1
    current_start_text: str = ""
    current_lines: List[str] = []

    for idx, line in enumerate(lines, start=1):
        start_match = START_RE.search(line)
        if start_match:
            if current_doc_id is not None:
                # Close previous block as incomplete before opening a new one.
                built = build_block_record(
                    file_name=file_name,
                    doc_id=current_doc_id,
                    start_line_number=current_start_line,
                    start_line_text=current_start_text,
                    end_line_number=idx - 1,
                    end_line_text="INCOMPLETE_BLOCK",
                    block_lines=current_lines,
                    max_error_lines=max_error_lines,
                )
                built["summary"]["status"] = "INCOMPLETE"
                built["details"]["status"] = "INCOMPLETE"
                records.append(built)

            current_doc_id = start_match.group("doc_id")
            current_start_line = idx
            current_start_text = line
            current_lines = [line]
            continue

        if current_doc_id is not None:
            current_lines.append(line)

            end_match = END_RE.search(line)
            if end_match and end_match.group("doc_id") == current_doc_id:
                built = build_block_record(
                    file_name=file_name,
                    doc_id=current_doc_id,
                    start_line_number=current_start_line,
                    start_line_text=current_start_text,
                    end_line_number=idx,
                    end_line_text=line,
                    block_lines=current_lines,
                    max_error_lines=max_error_lines,
                )
                records.append(built)

                current_doc_id = None
                current_start_line = -1
                current_start_text = ""
                current_lines = []

    if current_doc_id is not None:
        built = build_block_record(
            file_name=file_name,
            doc_id=current_doc_id,
            start_line_number=current_start_line,
            start_line_text=current_start_text,
            end_line_number=len(lines),
            end_line_text="INCOMPLETE_BLOCK",
            block_lines=current_lines,
            max_error_lines=max_error_lines,
        )
        built["summary"]["status"] = "INCOMPLETE"
        built["details"]["status"] = "INCOMPLETE"
        records.append(built)

    return records


def write_summary_csv(path: Path, rows: List[Dict[str, object]]) -> None:
    fieldnames = [
        "source_file_name",
        "doc_id",
        "start_line",
        "end_line",
        "start_timestamp",
        "end_timestamp",
        "has_indata",
        "has_outdata",
        "has_jcocall",
        "contains_all_blocks",
        "missing_blocks",
        "status",
        "error_count",
        "error_codes",
        "error_messages",
    ]

    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def write_details_json(path: Path, rows: List[Dict[str, object]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        json.dump(rows, handle, ensure_ascii=False, indent=2)


def build_request_document_summary(rows: List[Dict[str, object]]) -> List[Dict[str, object]]:
    grouped: Dict[str, Dict[str, object]] = {}

    for row in rows:
        doc_id = str(row.get("doc_id", "")).strip()
        if not doc_id:
            continue

        if doc_id not in grouped:
            grouped[doc_id] = {
                "request_document_id": doc_id,
                "contains_rfc_call": False,
                "contains_indata": False,
                "contains_outdata": False,
                "contains_all_blocks": False,
                "missing_blocks": "",
                "transaction_count": 0,
                "source_files": set(),
            }

        item = grouped[doc_id]
        item["contains_rfc_call"] = bool(item["contains_rfc_call"] or row.get("has_jcocall"))
        item["contains_indata"] = bool(item["contains_indata"] or row.get("has_indata"))
        item["contains_outdata"] = bool(item["contains_outdata"] or row.get("has_outdata"))
        item["transaction_count"] = int(item["transaction_count"]) + 1

        source_file_name = str(row.get("source_file_name", "")).strip()
        if source_file_name:
            item["source_files"].add(source_file_name)

    summary_rows: List[Dict[str, object]] = []
    for doc_id in sorted(grouped.keys()):
        item = grouped[doc_id]
        missing_blocks: List[str] = []
        if not item["contains_rfc_call"]:
            missing_blocks.append("rfc_call")
        if not item["contains_indata"]:
            missing_blocks.append("indata")
        if not item["contains_outdata"]:
            missing_blocks.append("outdata")

        item["contains_all_blocks"] = not missing_blocks
        item["missing_blocks"] = " | ".join(missing_blocks)
        item["source_files"] = " | ".join(sorted(item["source_files"]))
        summary_rows.append(item)

    return summary_rows


def write_request_document_summary_csv(path: Path, rows: List[Dict[str, object]]) -> None:
    fieldnames = [
        "request_document_id",
        "contains_rfc_call",
        "contains_indata",
        "contains_outdata",
        "contains_all_blocks",
        "missing_blocks",
        "transaction_count",
        "source_files",
    ]

    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def write_excel_rows(path: Path, headers: List[str], rows: List[Dict[str, object]]) -> None:
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "data"

    sheet.append(headers)
    for row in rows:
        sheet.append([row.get(header, "") for header in headers])

    sheet.freeze_panes = "A2"
    workbook.save(path)


def build_indata_excel_rows(details_rows: List[Dict[str, object]]) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    for details in details_rows:
        blocks = details.get("indata_blocks") or []
        if not isinstance(blocks, list):
            continue
        for index, block_text in enumerate(blocks, start=1):
            rows.append(
                {
                    "source_file_name": details.get("source_file_name", ""),
                    "doc_id": details.get("doc_id", ""),
                    "start_line": details.get("start_line", ""),
                    "end_line": details.get("end_line", ""),
                    "block_index": index,
                    "indata_block": block_text,
                }
            )
    return rows


def build_outdata_excel_rows(details_rows: List[Dict[str, object]]) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    for details in details_rows:
        blocks = details.get("outdata_blocks") or []
        if not isinstance(blocks, list):
            continue
        for index, block_text in enumerate(blocks, start=1):
            rows.append(
                {
                    "source_file_name": details.get("source_file_name", ""),
                    "doc_id": details.get("doc_id", ""),
                    "start_line": details.get("start_line", ""),
                    "end_line": details.get("end_line", ""),
                    "block_index": index,
                    "outdata_block": block_text,
                }
            )
    return rows


def build_jco_request_excel_rows(details_rows: List[Dict[str, object]]) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    for details in details_rows:
        lines = details.get("jcocall_lines") or []
        if not isinstance(lines, list):
            continue
        for index, jco_line in enumerate(lines, start=1):
            rows.append(
                {
                    "source_file_name": details.get("source_file_name", ""),
                    "doc_id": details.get("doc_id", ""),
                    "start_line": details.get("start_line", ""),
                    "end_line": details.get("end_line", ""),
                    "request_index": index,
                    "jco_request_line": jco_line,
                }
            )
    return rows


def build_request_presence_excel_rows(request_rows: List[Dict[str, object]]) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    for item in request_rows:
        rows.append(
            {
                "request_document_id": item.get("request_document_id", ""),
                "has_indata": item.get("contains_indata", False),
                "has_outdata": item.get("contains_outdata", False),
                "has_jco_request": item.get("contains_rfc_call", False),
                "contains_all_blocks": item.get("contains_all_blocks", False),
                "missing_blocks": item.get("missing_blocks", ""),
                "transaction_count": item.get("transaction_count", 0),
                "source_files": item.get("source_files", ""),
            }
        )
    return rows


def main() -> int:
    args = parse_args()

    input_dir = Path(args.input_dir)
    output_dir = DEFAULT_OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    zip_files = list(iter_log_files(input_dir))
    print(f"Startup: input directory = {input_dir}")
    print(f"Startup: found {len(zip_files)} .log.zip files")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    summary_path = Path(args.output_summary_csv) if args.output_summary_csv else output_dir / f"sabrix_request_document_summary_{timestamp}.csv"
    details_path = Path(args.output_details_json) if args.output_details_json else output_dir / f"sabrix_doc_block_details_{timestamp}.json"
    indata_excel_path = output_dir / f"sabrix_indata_blocks_{timestamp}.xlsx"
    outdata_excel_path = output_dir / f"sabrix_outdata_blocks_{timestamp}.xlsx"
    jco_request_excel_path = output_dir / f"sabrix_jco_request_blocks_{timestamp}.xlsx"
    request_presence_excel_path = output_dir / f"sabrix_request_presence_{timestamp}.xlsx"

    all_summary_rows: List[Dict[str, object]] = []
    all_detail_rows: List[Dict[str, object]] = []
    xml_payload_count = 0

    for log_path in zip_files:
        for inner_name, text in iter_zip_xml_payloads(log_path):
            if not text:
                continue

            xml_payload_count += 1
            source_name = f"{log_path.name}:{inner_name}"
            records = extract_blocks_from_text(text, source_name, args.max_error_lines)
            for record in records:
                all_summary_rows.append(record["summary"])
                all_detail_rows.append(record["details"])

    print(f"Startup: processed {xml_payload_count} XML payload file(s) from zip archives")

    request_document_summary = build_request_document_summary(all_summary_rows)

    write_summary_csv(summary_path, all_summary_rows)
    request_document_summary_path = summary_path.with_name(summary_path.stem + "_by_request_document_id.csv")
    write_request_document_summary_csv(request_document_summary_path, request_document_summary)
    write_details_json(details_path, all_detail_rows)

    indata_excel_rows = build_indata_excel_rows(all_detail_rows)
    outdata_excel_rows = build_outdata_excel_rows(all_detail_rows)
    jco_request_excel_rows = build_jco_request_excel_rows(all_detail_rows)
    request_presence_rows = build_request_presence_excel_rows(request_document_summary)

    write_excel_rows(
        indata_excel_path,
        ["source_file_name", "doc_id", "start_line", "end_line", "block_index", "indata_block"],
        indata_excel_rows,
    )
    write_excel_rows(
        outdata_excel_path,
        ["source_file_name", "doc_id", "start_line", "end_line", "block_index", "outdata_block"],
        outdata_excel_rows,
    )
    write_excel_rows(
        jco_request_excel_path,
        ["source_file_name", "doc_id", "start_line", "end_line", "request_index", "jco_request_line"],
        jco_request_excel_rows,
    )
    write_excel_rows(
        request_presence_excel_path,
        [
            "request_document_id",
            "has_indata",
            "has_outdata",
            "has_jco_request",
            "contains_all_blocks",
            "missing_blocks",
            "transaction_count",
            "source_files",
        ],
        request_presence_rows,
    )

    total = len(all_summary_rows)
    complete = len([row for row in all_summary_rows if str(row.get("status", "")).upper() == "COMPLETE"])
    incomplete = len([row for row in all_summary_rows if str(row.get("status", "")).upper() == "INCOMPLETE"])

    print(f"Input directory: {input_dir}")
    print(f"Transactions found: {total}")
    print(f"Complete block transactions: {complete}")
    print(f"Incomplete transactions: {incomplete}")
    print(f"Transaction summary CSV: {summary_path}")
    print(f"Request document summary CSV: {request_document_summary_path}")
    print(f"Details JSON: {details_path}")
    print(f"INDATA blocks Excel: {indata_excel_path}")
    print(f"OUTDATA blocks Excel: {outdata_excel_path}")
    print(f"JCO request blocks Excel: {jco_request_excel_path}")
    print(f"Request presence Excel: {request_presence_excel_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
