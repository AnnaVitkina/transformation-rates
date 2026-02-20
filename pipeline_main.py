"""
Single pipeline entrypoint for the full DHL rate-card flow.

Designed to run locally and in Google Colab (including exec(open(...).read())).
"""

import argparse
import contextlib
import io
import json
import os
import shutil
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# HARDCODED DEFAULT PATHS (edit these once for smoother Colab usage)
# Priority order remains: CLI args > env vars > hardcoded defaults.
# ---------------------------------------------------------------------------
HARDCODED_INPUT_FOLDER = "/content/drive/MyDrive/RMT test/Output"
HARDCODED_ARCHIVE_FOLDER = "/content/drive/MyDrive/RMT/archive"
HARDCODED_CLIENTS_FILE = "/content/drive/MyDrive/RMT/additional info/clients.txt"
HARDCODED_COUNTRY_CODES_FILE = "/content/drive/MyDrive/RMT/additional info/dhl_country_codes.txt"
HARDCODED_ACCESSORIAL_FILE = "/content/drive/MyDrive/RMT/additional info/Accessorial Costs.xlsx"
HARDCODED_OUTPUT_DIR = "/content/drive/MyDrive/RMT/output"


def _detect_project_root():
    """Best-effort project root detection (works in Colab exec and normal runs)."""
    candidates = []
    env_root = os.environ.get("REPO_ROOT")
    if env_root:
        candidates.append(Path(env_root))
    if "__file__" in globals():
        candidates.append(Path(__file__).resolve().parent)
    candidates.append(Path.cwd())
    candidates.append(Path("/content/transformation-rate"))
    candidates.append(Path("/content/transformation-rates"))

    # Also scan /content/* for a repo-like folder (Colab-friendly)
    content_root = Path("/content")
    if content_root.exists():
        for child in content_root.iterdir():
            if child.is_dir():
                candidates.append(child)

    for c in candidates:
        if (c / "create_table.py").exists() and (c / "main.py").exists():
            return c.resolve()
    return Path.cwd().resolve()


PROJECT_ROOT = _detect_project_root()
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import create_table
import fill_service_types
import main as extractor
from country_region_txt_creation import create_country_region_txt


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run end-to-end DHL extraction and output generation pipeline."
    )
    parser.add_argument(
        "--input-file",
        default=None,
        help="Input Azure DI JSON path (can be on Google Drive).",
    )
    parser.add_argument(
        "--input-folder",
        default=None,
        help="Folder containing JSON files. Script will list them and ask you to choose one.",
    )
    parser.add_argument(
        "--archive-folder",
        default=None,
        help="Archive folder path for processed input JSON. Default: <input-folder>/archive",
    )
    parser.add_argument(
        "--clients-file",
        default=None,
        help="Clients file path (one client per line).",
    )
    parser.add_argument(
        "--country-codes-file",
        default=None,
        help="Country codes file path (Country<TAB>Code).",
    )
    parser.add_argument(
        "--accessorial-file",
        default=None,
        help="Accessorial costs reference file path (.xlsx or .csv, must have Name column).",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Directory to write outputs (xlsx/txt/extracted json).",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Show full debug output from underlying modules.",
    )
    # parse_known_args avoids failures in Colab where extra argv flags are present
    args, _unknown = parser.parse_known_args()
    return args


def _list_json_files(folder_path):
    folder = Path(folder_path)
    if not folder.exists() or not folder.is_dir():
        raise FileNotFoundError(f"Input folder not found: {folder}")
    files = sorted(folder.glob("*.json"), key=lambda p: p.name.lower())
    if not files:
        raise FileNotFoundError(f"No .json files found in: {folder}")
    return files


def _choose_json_from_folder(folder_path):
    files = _list_json_files(folder_path)
    print("Select input JSON file:")
    print()
    for i, p in enumerate(files, 1):
        size_mb = p.stat().st_size / (1024 * 1024)
        print(f"  {i}. {p.name}  ({size_mb:.2f} MB)")
    print()
    while True:
        choice = input(f"Enter number (1-{len(files)}): ").strip()
        try:
            n = int(choice)
            if 1 <= n <= len(files):
                return files[n - 1]
        except ValueError:
            pass
        print("Invalid choice. Enter a number from the list.")


def resolve_input_file(input_arg, input_folder_arg=None):
    """
    Resolve input file:
    - If not provided: interactive selection from input/
    - If bare file name: resolve under input/
    - Else: use as provided
    """
    if input_arg is None:
        env_input_folder = os.environ.get("INPUT_FOLDER")
        folder = input_folder_arg or env_input_folder or HARDCODED_INPUT_FOLDER
        if folder:
            selected = _choose_json_from_folder(folder)
            return str(selected), str(Path(folder))
        env_input = os.environ.get("INPUT_FILE")
        if env_input:
            return env_input, None
        return extractor.choose_input_file_interactive(), None
    p = Path(input_arg)
    if not p.is_absolute() and len(p.parts) == 1:
        return str(extractor.INPUT_DIR / p), None
    return str(p), None


def _archive_processed_input(input_file, input_folder=None, archive_folder=None):
    """
    Move processed input file to archive.
    - If archive_folder provided, use it.
    - Else if input_folder provided, use <input_folder>/archive.
    - Else: do nothing.
    """
    if archive_folder is None:
        archive_folder = os.environ.get("ARCHIVE_FOLDER")
    if archive_folder is None:
        archive_folder = HARDCODED_ARCHIVE_FOLDER
    if archive_folder is None and input_folder:
        archive_folder = str(Path(input_folder) / "archive")
    if not archive_folder:
        return None

    src = Path(input_file)
    if not src.exists():
        return None

    archive_dir = Path(archive_folder)
    archive_dir.mkdir(parents=True, exist_ok=True)
    dst = archive_dir / src.name
    if dst.exists():
        stem = src.stem
        suffix = src.suffix
        i = 1
        while True:
            candidate = archive_dir / f"{stem}_{i}{suffix}"
            if not candidate.exists():
                dst = candidate
                break
            i += 1
    shutil.move(str(src), str(dst))
    return str(dst)


def _prepare_reference_files(country_codes_file, accessorial_file):
    """
    Copy externally-provided reference files into the file names expected by existing modules.
    This avoids changing internal logic in create_table.py.
    """
    # Country codes expected by create_table._load_country_codes():
    #   input/dhl_country_codes.txt (first choice)
    if country_codes_file:
        src = Path(country_codes_file)
        if src.exists():
            dst_dir = PROJECT_ROOT / "input"
            dst_dir.mkdir(parents=True, exist_ok=True)
            dst = dst_dir / "dhl_country_codes.txt"
            shutil.copy2(src, dst)
            print(f"[OK] Country codes staged: {dst}")
        else:
            raise FileNotFoundError(f"Country codes file not found: {src}")

    # Accessorial file expected by create_table.build_accessorial_costs_rows():
    #   addition/Accessorial Costs.xlsx OR addition/Accessorial Costs.csv
    if accessorial_file:
        src = Path(accessorial_file)
        if not src.exists():
            raise FileNotFoundError(f"Accessorial file not found: {src}")
        suffix = src.suffix.lower()
        if suffix not in (".xlsx", ".xls", ".csv"):
            raise ValueError("Accessorial file must be .xlsx/.xls or .csv")
        dst_dir = PROJECT_ROOT / "addition"
        dst_dir.mkdir(parents=True, exist_ok=True)
        if suffix in (".xlsx", ".xls"):
            dst = dst_dir / "Accessorial Costs.xlsx"
        else:
            dst = dst_dir / "Accessorial Costs.csv"
        shutil.copy2(src, dst)
        print(f"[OK] Accessorial reference staged: {dst}")


def run_pipeline(
    input_file,
    clients_file=None,
    country_codes_file=None,
    accessorial_file=None,
    output_dir=None,
    input_folder=None,
    archive_folder=None,
    verbose=False,
):
    if clients_file is None:
        clients_file = os.environ.get("CLIENTS_FILE")
    if clients_file is None:
        clients_file = HARDCODED_CLIENTS_FILE
    if country_codes_file is None:
        country_codes_file = os.environ.get("COUNTRY_CODES_FILE")
    if country_codes_file is None:
        country_codes_file = HARDCODED_COUNTRY_CODES_FILE
    if accessorial_file is None:
        accessorial_file = os.environ.get("ACCESSORIAL_FILE")
    if accessorial_file is None:
        accessorial_file = HARDCODED_ACCESSORIAL_FILE
    if output_dir is None:
        output_dir = os.environ.get("OUTPUT_DIR")
    if output_dir is None:
        output_dir = HARDCODED_OUTPUT_DIR

    output_root = Path(output_dir) if output_dir else (PROJECT_ROOT / "output")
    output_root.mkdir(parents=True, exist_ok=True)

    extracted_json_path = output_root / "extracted_data.json"
    output_xlsx_path = output_root / "DHL_Rate_Cards.xlsx"
    output_txt_path = output_root / "CountryZoning_by_RateName.txt"
    default_clients = PROJECT_ROOT / "addition" / "clients.txt"
    clients_path = Path(clients_file) if clients_file else default_clients

    print("=" * 70)
    print("DHL PIPELINE RUNNER")
    print("=" * 70)
    print(f"[*] Project root: {PROJECT_ROOT}")
    print(f"[*] Input: {input_file}")
    print(f"[*] Clients file: {clients_path}")
    print(f"[*] Output directory: {output_root}")
    if input_folder:
        print(f"[*] Input folder: {input_folder}")
    if archive_folder:
        print(f"[*] Archive folder: {archive_folder}")
    print()

    _prepare_reference_files(country_codes_file, accessorial_file)

    def _run_quiet(label, fn, *args, **kwargs):
        """Run function with captured stdout/stderr unless verbose=True."""
        if verbose:
            return fn(*args, **kwargs)
        out_buf = io.StringIO()
        err_buf = io.StringIO()
        try:
            with contextlib.redirect_stdout(out_buf), contextlib.redirect_stderr(err_buf):
                return fn(*args, **kwargs)
        except Exception:
            print(f"[ERROR] {label} failed.")
            captured = out_buf.getvalue().strip()
            captured_err = err_buf.getvalue().strip()
            if captured:
                print("---- Captured stdout (last lines) ----")
                print("\n".join(captured.splitlines()[-20:]))
            if captured_err:
                print("---- Captured stderr (last lines) ----")
                print("\n".join(captured_err.splitlines()[-20:]))
            raise

    # Step 1: Read clients and input JSON
    print("Step 1: Reading clients and input JSON...")
    client_list = _run_quiet("Read client list", extractor.read_client_list, str(clients_path))
    input_data = _run_quiet("Read input JSON", extractor.read_converted_json, input_file)
    print(f"[OK] Client names loaded: {len(client_list)}")
    print()

    # Step 2: Extract + transform + save extracted JSON
    print("Step 2: Extracting and transforming data...")
    client_name = _run_quiet("Detect client", extractor.detect_client_from_json, input_data, client_list)
    fields = _run_quiet("Extract fields", extractor.extract_fields, input_data)
    processed_data = _run_quiet("Transform data", extractor.transform_data, fields, client_name)
    _run_quiet("Save extracted JSON", extractor.save_output, processed_data, str(extracted_json_path))
    stats = processed_data.get("statistics", {})
    print(f"[OK] Client detected: {client_name}")
    print(
        f"[OK] Extracted rows: MainCosts={stats.get('MainCosts_rows', 0)}, "
        f"AddedRates={stats.get('AddedRates_rows', 0)}, "
        f"CountryZoning={stats.get('CountryZoning_rows', 0)}"
    )
    print()

    # Step 3: Fill null service types and persist
    print("Step 3: Filling null service_type values...")
    filled_count = fill_service_types.fill_null_service_types(processed_data)
    with open(extracted_json_path, "w", encoding="utf-8") as f:
        json.dump(processed_data, f, indent=2, ensure_ascii=False)
    print(f"[OK] Filled {filled_count} section(s)")
    print()

    # Step 4: Build Excel from extracted JSON object
    print("Step 4: Creating Excel workbook...")
    output_xlsx_path.parent.mkdir(parents=True, exist_ok=True)
    _run_quiet("Create Excel", create_table.save_to_excel, processed_data, str(output_xlsx_path))
    print(f"[OK] Excel created: {output_xlsx_path}")
    print()

    # Step 5: Build CountryZoning TXT from generated Excel
    print("Step 5: Creating CountryZoning TXT...")
    txt_out = _run_quiet(
        "Create CountryZoning TXT",
        create_country_region_txt,
        excel_path=str(output_xlsx_path),
        output_path=str(output_txt_path),
    )
    print(f"[OK] TXT saved: {txt_out}")
    print()

    print("=" * 70)
    print("[SUCCESS] PIPELINE COMPLETE")
    print("=" * 70)
    print(f"Client: {client_name}")
    print(f"Extracted JSON: {extracted_json_path}")
    print(f"Excel: {output_xlsx_path}")
    print(f"TXT: {output_txt_path}")
    archived_to = _archive_processed_input(
        input_file=input_file,
        input_folder=input_folder,
        archive_folder=archive_folder,
    )
    if archived_to:
        print(f"Archived input JSON: {archived_to}")
    print()
    print("Overall:")
    print(f"- Input processed: {input_file}")
    print(f"- Client: {client_name}")
    print(f"- JSON output: {extracted_json_path}")
    print(f"- Excel output: {output_xlsx_path}")
    print(f"- TXT output: {output_txt_path}")
    if archived_to:
        print(f"- Archived input: {archived_to}")
    print()


def main():
    args = parse_args()
    input_file, selected_folder = resolve_input_file(args.input_file, args.input_folder)
    run_pipeline(
        input_file=input_file,
        clients_file=args.clients_file,
        country_codes_file=args.country_codes_file,
        accessorial_file=args.accessorial_file,
        output_dir=args.output_dir,
        input_folder=selected_folder or args.input_folder,
        archive_folder=args.archive_folder,
        verbose=args.verbose,
    )


if __name__ == "__main__":
    main()

