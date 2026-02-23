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
HARDCODED_INPUT_FOLDER = "/content/drive/Shareddrives/FA Ops Europe: Rate Maintenance Team /Documents/AI Adoption RMT/RMT/input json"
HARDCODED_ARCHIVE_FOLDER = "/content/drive/Shareddrives/FA Ops Europe: Rate Maintenance Team /Documents/AI Adoption RMT/RMT/archive"
HARDCODED_CLIENTS_FILE = "/content/drive/Shareddrives/FA Ops Europe: Rate Maintenance Team /Documents/AI Adoption RMT/RMT/addition/clients.txt"
HARDCODED_COUNTRY_CODES_FILE = "/content/drive/Shareddrives/FA Ops Europe: Rate Maintenance Team /Documents/AI Adoption RMT/RMT/addition/dhl_country_codes.txt"
HARDCODED_ACCESSORIAL_FILE = "/content/drive/Shareddrives/FA Ops Europe: Rate Maintenance Team /Documents/AI Adoption RMT/RMT/addition/Accessorial Costs.xlsx"
HARDCODED_OUTPUT_DIR = "/content/drive/Shareddrives/FA Ops Europe: Rate Maintenance Team /Documents/AI Adoption RMT/RMT/output"

# Local (Windows) – used when Drive path does not exist
LOCAL_INPUT_FOLDER = r"C:\Users\avitkin\.cursor\projects_folders\RMT\tranformation-rate\input"
LOCAL_ARCHIVE_FOLDER = r"C:\Users\avitkin\.cursor\projects_folders\RMT\tranformation-rate\archive"
LOCAL_CLIENTS_FILE = r"C:\Users\avitkin\.cursor\projects_folders\RMT\tranformation-rate\addition\clients.txt"
LOCAL_COUNTRY_CODES_FILE = r"C:\Users\avitkin\.cursor\projects_folders\RMT\tranformation-rate\addition\dhl_country_codes.docx"
LOCAL_ACCESSORIAL_FILE = r"C:\Users\avitkin\.cursor\projects_folders\RMT\tranformation-rate\addition\Accessorial Costs.xlsx"
LOCAL_OUTPUT_DIR = r"C:\Users\avitkin\.cursor\projects_folders\RMT\tranformation-rate\output"


def _drive_available():
    """True if the Drive input folder exists (Colab with Drive mounted). Else we run and save on local machine."""
    p = Path(HARDCODED_INPUT_FOLDER)
    return p.exists() and p.is_dir()


def _use_drive_or_local(path_str, local_fallback, is_dir=False):
    """Use path_str if it exists (Drive); else use local_fallback for local execution. Drive logic unchanged."""
    if path_str:
        p = Path(path_str)
        if is_dir:
            if p.exists() and p.is_dir():
                return path_str
        else:
            if p.exists():
                return path_str
    if local_fallback:
        print(f"[*] Using local path (Drive not available): {local_fallback}")
    return local_fallback or path_str


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
        help="Accessorial costs reference file path (optional). By default, addition/Accessorial Costs <ClientName>.xlsx is used per client.",
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
            folder = _use_drive_or_local(folder, LOCAL_INPUT_FOLDER, is_dir=True)
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
    Use reference files in place. create_table reads country codes from input/ or addition/,
    and Accessorial from addition/. Copy only when the file is outside those locations (e.g. Drive root).
    """
    # Country codes: create_table looks at input/dhl_country_codes.txt then addition/dhl_country_codes.txt
    if country_codes_file:
        src = Path(country_codes_file)
        if not src.exists():
            raise FileNotFoundError(f"Country codes file not found: {src}")
        in_input = (PROJECT_ROOT / "input" / "dhl_country_codes.txt").resolve() == src.resolve()
        in_addition = (PROJECT_ROOT / "addition" / "dhl_country_codes.txt").resolve() == src.resolve()
        if in_input or in_addition:
            print(f"[OK] Country codes used in place: {src}")
        else:
            dst_dir = PROJECT_ROOT / "input"
            dst_dir.mkdir(parents=True, exist_ok=True)
            dst = dst_dir / "dhl_country_codes.txt"
            shutil.copy2(src, dst)
            print(f"[OK] Country codes staged: {dst}")

    # Accessorial: optional. If path exists, stage it; else skip (create_table will use addition/ by client or generic).
    if accessorial_file:
        src = Path(accessorial_file)
        if not src.exists():
            print(f"[*] Accessorial file not found at {src}; create_table will use addition/ (client-specific or generic).")
        else:
            suffix = src.suffix.lower()
            if suffix not in (".xlsx", ".xls", ".csv"):
                raise ValueError("Accessorial file must be .xlsx/.xls or .csv")
            dst_dir = PROJECT_ROOT / "addition"
            dst_name = "Accessorial Costs.xlsx" if suffix in (".xlsx", ".xls") else "Accessorial Costs.csv"
            dst = dst_dir / dst_name
            if src.resolve() == dst.resolve():
                print(f"[OK] Accessorial used in place: {src}")
            else:
                dst_dir.mkdir(parents=True, exist_ok=True)
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

    # If Drive input is not available, run and save entirely on local machine
    if not _drive_available():
        print("[*] Drive not available; running and saving on local machine.")
        if input_folder in (None, HARDCODED_INPUT_FOLDER):
            input_folder = LOCAL_INPUT_FOLDER
        if clients_file == HARDCODED_CLIENTS_FILE:
            clients_file = LOCAL_CLIENTS_FILE
        if country_codes_file == HARDCODED_COUNTRY_CODES_FILE:
            country_codes_file = LOCAL_COUNTRY_CODES_FILE
        if accessorial_file == HARDCODED_ACCESSORIAL_FILE:
            accessorial_file = LOCAL_ACCESSORIAL_FILE
        if output_dir == HARDCODED_OUTPUT_DIR:
            output_dir = LOCAL_OUTPUT_DIR
        if archive_folder in (None, HARDCODED_ARCHIVE_FOLDER):
            archive_folder = LOCAL_ARCHIVE_FOLDER
    else:
        # Drive available: use paths as resolved (Drive or env/CLI)
        input_folder = _use_drive_or_local(input_folder, LOCAL_INPUT_FOLDER, is_dir=True) if input_folder else input_folder
        clients_file = _use_drive_or_local(clients_file, LOCAL_CLIENTS_FILE)
        country_codes_file = _use_drive_or_local(country_codes_file, LOCAL_COUNTRY_CODES_FILE)
        accessorial_file = _use_drive_or_local(accessorial_file, LOCAL_ACCESSORIAL_FILE)
        output_dir = _use_drive_or_local(output_dir, LOCAL_OUTPUT_DIR, is_dir=True)
        if archive_folder:
            archive_folder = _use_drive_or_local(archive_folder, LOCAL_ARCHIVE_FOLDER, is_dir=True)

    output_root = Path(output_dir) if output_dir else (PROJECT_ROOT / "output")
    output_root.mkdir(parents=True, exist_ok=True)

    # Name outputs after input file; if file exists, use stem_1, stem_2, ...
    input_stem = Path(input_file).stem

    def _unique_path(directory, base_stem, suffix):
        """Return path that does not exist yet: base_stem + suffix, or base_stem_1 + suffix, etc."""
        candidate = directory / f"{base_stem}{suffix}"
        if not candidate.exists():
            return candidate
        for i in range(1, 10000):
            candidate = directory / f"{base_stem}_{i}{suffix}"
            if not candidate.exists():
                return candidate
        raise RuntimeError(f"Could not find unique path for {base_stem}{suffix}")

    output_xlsx_path = _unique_path(output_root, input_stem, ".xlsx")
    output_txt_path = _unique_path(output_root, input_stem + "_CountryZoning_by_RateName", ".txt")
    extracted_json_path = output_root / "extracted_data.json"
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
    FileName = Path(input_file).name
    processed_data.setdefault("metadata", {})["FileName"] = FileName
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










