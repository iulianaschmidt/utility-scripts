# SOAP Batch Tax Processor

Batch SOAP Tax Calculation Processor for OneSource/Sabrix tax calls.

This script processes production-style data (CSV/Excel), builds SOAP XML requests, sends them to the tax calculation endpoint, and saves both summary results and parsed `OUTDATA` output.

It supports:

- **Batch mode** (normal processing)
- **Load-test mode** (high concurrency performance testing)
- **Parallel execution**
- **Row-level or document-level request granularity**
- **Detailed error capture and logging**
- **OUTDATA extraction into flattened CSV format**

---

## 1) What this script does

`soap_batch_tax_processor.py`:

1. Loads input data from CSV/XLSX.
2. Builds SOAP requests from each row (or grouped by `REQUEST_DOCUMENT_ID`).
3. Sends requests to configured OneSource SOAP endpoint.
4. Parses XML response:
   - request success
   - tax amount
   - OUTDATA JSON
   - flattened OUTDATA columns
5. Writes output files:
   - processing summary CSV
   - OUTDATA CSV
   - log file
6. Optionally runs load tests and outputs latency/failure statistics.

---

## 2) Requirements

- **Python 3.8+** (recommended)
- Packages:
  - `pandas`
  - `requests`
  - `openpyxl` (if using `.xlsx` input)

Install dependencies:

```bash
pip install pandas requests openpyxl
```

---

## 3) Project setup (recommended)

From repository root:

```bash
py -3.8 -m venv .venv
.venv\Scripts\activate
pip install pandas requests openpyxl
```

> The script includes `ensure_project_venv()` and will relaunch with `.venv\Scripts\python.exe` automatically if found.

---

## 4) Configuration

Edit the **configuration section** near the top of the script:

- `SOAP_ENDPOINT`
- `SOAP_ACTION`
- `SOAP_USERNAME`
- `SOAP_PASSWORD`
- File paths:
  - `INPUT_FILE`
  - `OUTPUT_FILE`
  - `OUTDATA_OUTPUT_FILE`
  - `LOG_FILE`
  - Load-test files (`LOAD_TEST_RESULTS_FILE`, etc.)
- Runtime defaults:
  - `MAX_WORKERS`
  - `REQUEST_TIMEOUT`

> ⚠️ Security note: credentials are currently hardcoded. Prefer environment variables for sensitive values in shared or production environments.

---

## 5) Input data expectations

Input can be:

- `.csv`
- `.xlsx`

Typical columns used include:

- `REQUEST_DOCUMENT_ID`
- `INVOICE_NUMBER`
- `INVOICE_DATE`
- `EXTERNAL_COMPANY_ID`
- `CURRENCY_CODE`
- `CUSTOMER_NUMBER`
- `GROSS_AMOUNT`
- `SHIP_TO_COUNTRY`, `SHIP_TO_STATE`, `SHIP_TO_POSTCODE`, etc.
- `LINE_NUMBER`, `LINE_ID`
- `ATTRIBUTE1`, `ATTRIBUTE24`, `ATTRIBUTE48`, etc.

The script tolerates multiple aliases (`UPPER_CASE` and `snake_case`) in many places.

---

## 6) Running process

### A) Batch mode (default)

#### Command

```bash
python soap_batch_tax_processor.py --mode batch
```

#### Recommended explicit command

```bash
python soap_batch_tax_processor.py ^
  --mode batch ^
  --input-file "C:\path\to\input.csv" ^
  --output-file "C:\path\to\response_results.csv" ^
  --outdata-output-file "C:\path\to\outdata_results.csv" ^
  --max-workers 5 ^
  --request-granularity row
```

#### Important options

- `--request-granularity row`
  - 1 SOAP request per input row
- `--request-granularity document`
  - 1 SOAP request per `REQUEST_DOCUMENT_ID` group (multiple LINEs per request)
- `--max-workers`
  - parallel worker count

---

### B) Load-test mode

#### Command

```bash
python soap_batch_tax_processor.py --mode load-test
```

#### Recommended explicit command

```bash
python soap_batch_tax_processor.py ^
  --mode load-test ^
  --input-file "C:\path\to\input.csv" ^
  --load-requests 1000 ^
  --load-concurrency 200 ^
  --load-source cycle ^
  --load-results-file "C:\path\to\load_results.csv" ^
  --load-summary-file "C:\path\to\load_summary.json" ^
  --load-failures-file "C:\path\to\load_failures.csv"
```

#### Load-test options

- `--load-requests`: total requests to execute
- `--load-concurrency`: concurrent workers
- `--load-source`:
  - `cycle`: rotates through grouped documents
  - `first`: repeats first grouped document
- Output files for results, summary, failures

---

## 7) Output files

### Batch mode

1. **Summary CSV** (`--output-file`)
   - row index
   - invoice number
   - status
   - HTTP status
   - success flag
   - total tax
   - response time
   - error fields

2. **OUTDATA CSV** (`--outdata-output-file`)
   - flattened OUTDATA fields
   - `outdata_mapped_json` full structured snapshot

3. **Log file** (`LOG_FILE`)
   - progress
   - request-level errors
   - completion summary

### Load-test mode

1. **Results CSV** (`--load-results-file`)
2. **Failures CSV** (`--load-failures-file`)
3. **Summary JSON** (`--load-summary-file`) including:
   - failure rate
   - throughput
   - p50/p90/p95/p99 latency
   - HTTP status counts

---

## 8) Failure handling and diagnostics

The script captures:

- Timeout failures (`requests.Timeout`)
- HTTP failures (including HTTP 500 SOAP faults)
- XML parsing failures
- Unexpected exceptions with traceback snippets

Useful fields in output/logs:

- `error_type`
- `error_message`
- `error_details`
- `response_excerpt`

---

## 9) Example run workflow (end-to-end)

1. Update endpoint/credentials and file paths in config.
2. Place your input file (`.csv`/`.xlsx`) at desired path.
3. Run batch mode with `--request-granularity row` first.
4. Check:
   - summary CSV
   - OUTDATA CSV
   - log file
5. If needed, rerun with `--request-granularity document` for grouped invoice behavior.
6. Run load test to validate throughput and failure profile.

---

## 10) Notes and cautions

- SSL verification is currently disabled in request calls (`verify=False`).
  - Use with caution; enable certificate verification for stricter environments.
- Credentials should be externalized (env vars / secret manager).
- Windows paths are hardcoded in defaults; prefer CLI args for portability.

---

## 11) Quick command reference

```bash
# Batch (row-level)
python soap_batch_tax_processor.py --mode batch --request-granularity row

# Batch (document-level)
python soap_batch_tax_processor.py --mode batch --request-granularity document

# Batch with custom workers
python soap_batch_tax_processor.py --mode batch --max-workers 10

# Load test
python soap_batch_tax_processor.py --mode load-test --load-requests 1000 --load-concurrency 200
```
