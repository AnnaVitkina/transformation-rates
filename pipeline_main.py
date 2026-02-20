"""
Single pipeline entrypoint for the full DHL rate-card flow.

Designed to run locally and in Google Colab (including exec(open(...).read())).
"""

import argparse
import json
import os
import shutil
import sys
from pathlib import Path


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
    # parse_known_args avoids failures in Colab where extra argv flags are present
    args, _unknown = parser.parse_known_args()
    return args


def resolve_input_file(input_arg):
    """
    Resolve input file:
    - If not provided: interactive selection from input/
    - If bare file name: resolve under input/
    - Else: use as provided
    """
    if input_arg is None:
        env_input = os.environ.get("INPUT_FILE")
        if env_input:
            return env_input
        return extractor.choose_input_file_interactive()
    p = Path(input_arg)
    if not p.is_absolute() and len(p.parts) == 1:
        return str(extractor.INPUT_DIR / p)
    return str(p)


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


def run_pipeline(input_file, clients_file=None, country_codes_file=None, accessorial_file=None, output_dir=None):
    if clients_file is None:
        clients_file = os.environ.get("CLIENTS_FILE")
    if country_codes_file is None:
        country_codes_file = os.environ.get("COUNTRY_CODES_FILE")
    if accessorial_file is None:
        accessorial_file = os.environ.get("ACCESSORIAL_FILE")
    if output_dir is None:
        output_dir = os.environ.get("OUTPUT_DIR")

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
    print()

    _prepare_reference_files(country_codes_file, accessorial_file)

    # Step 1: Read clients and input JSON
    print("Step 1: Reading clients and input JSON...")
    client_list = extractor.read_client_list(str(clients_path))
    input_data = extractor.read_converted_json(input_file)
    print()

    # Step 2: Extract + transform + save extracted JSON
    print("Step 2: Extracting and transforming data...")
    client_name = extractor.detect_client_from_json(input_data, client_list)
    fields = extractor.extract_fields(input_data)
    processed_data = extractor.transform_data(fields, client_name)
    extractor.save_output(processed_data, str(extracted_json_path))
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
    create_table.save_to_excel(processed_data, str(output_xlsx_path))
    print()

    # Step 5: Build CountryZoning TXT from generated Excel
    print("Step 5: Creating CountryZoning TXT...")
    txt_out = create_country_region_txt(
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
    print()


def main():
    args = parse_args()
    input_file = resolve_input_file(args.input_file)
    run_pipeline(
        input_file=input_file,
        clients_file=args.clients_file,
        country_codes_file=args.country_codes_file,
        accessorial_file=args.accessorial_file,
        output_dir=args.output_dir,
    )


if __name__ == "__main__":
    main()
