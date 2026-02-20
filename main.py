"""
DHL Rate Card Data Extractor
Extracts structured data from Azure Document Intelligence JSON output
"""

import argparse
import json
import os
from datetime import datetime
from pathlib import Path


def read_converted_json(filepath):
    """Read and parse the Azure Document Intelligence JSON file"""
    print(f"[*] Reading JSON file: {filepath}")
    try:
        file_size_mb = os.path.getsize(filepath) / (1024 * 1024)
        print(f"    [DEBUG] File size: {file_size_mb:.2f} MB")
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        print(f"[OK] Successfully loaded JSON file")
        top_keys = list(data.keys()) if isinstance(data, dict) else []
        print(f"    [DEBUG] Top-level keys: {top_keys}")
        if 'analyzeResult' in data:
            ar = data['analyzeResult']
            print(f"    [DEBUG] analyzeResult keys: {list(ar.keys())}")
            if 'documents' in ar:
                print(f"    [DEBUG] Number of documents: {len(ar['documents'])}")
            if 'content' in ar:
                content_len = len(ar.get('content', ''))
                print(f"    [DEBUG] Content length (chars): {content_len:,}")
        return data
    except FileNotFoundError:
        print(f"[ERROR] File not found: {filepath}")
        raise
    except json.JSONDecodeError as e:
        print(f"[ERROR] Invalid JSON format: {e}")
        raise
    except Exception as e:
        print(f"[ERROR] Reading file: {e}")
        raise


def extract_fields(data):
    """Extract fields from analyzeResult.documents[0].fields"""
    print("[*] Extracting fields from document...")
    try:
        analyze_result = data.get('analyzeResult', {})
        documents = analyze_result.get('documents', [])
        
        if not documents:
            print("[WARN] No documents found in analyzeResult")
            return {}
        
        fields = documents[0].get('fields', {})
        print(f"[OK] Found {len(fields)} top-level fields")
        field_names = list(fields.keys())
        print(f"    [DEBUG] Field names: {field_names}")
        for fn in field_names:
            fv = fields[fn]
            ftype = fv.get('type', '?') if isinstance(fv, dict) else type(fv).__name__
            if ftype == 'array':
                arr = fv.get('valueArray', [])
                print(f"    [DEBUG]   {fn}: type={ftype}, length={len(arr)}")
            else:
                val = extract_value(fv) if isinstance(fv, dict) else fv
                preview = str(val)[:50] + "..." if val and len(str(val)) > 50 else val
                print(f"    [DEBUG]   {fn}: type={ftype}, value={repr(preview)}")
        return fields
    except Exception as e:
        print(f"[ERROR] Extracting fields: {e}")
        return {}


def extract_value(field):
    """Extract the actual value from a field object"""
    if not field:
        return None
    
    # Try different value types
    if 'valueString' in field:
        return field['valueString']
    elif 'content' in field:
        return field['content']
    elif 'valueNumber' in field:
        return field['valueNumber']
    elif 'valueDate' in field:
        return field['valueDate']
    
    return None


def process_main_costs_item(value_object, is_header=False):
    """Process a single MainCosts item (either header or data row)"""
    result = {}
    
    # Extract all zone fields dynamically
    zones = {}
    for key, value in value_object.items():
        if key.startswith('Zone'):
            zone_value = extract_value(value)
            if zone_value:
                zones[key] = zone_value
        elif key in ['RateName', 'CostName', 'Weight']:
            result[key] = extract_value(value)
    
    if zones:
        result['zones'] = zones
    
    return result


def process_main_costs(main_costs_field):
    """Process the MainCosts array into structured rate cards"""
    print("[*] Processing MainCosts data...")
    
    if not main_costs_field or main_costs_field.get('type') != 'array':
        print("[WARN] MainCosts is not an array")
        return []
    
    value_array = main_costs_field.get('valueArray', [])
    if not value_array:
        print("[WARN] MainCosts valueArray is empty")
        return []
    
    print(f"    [DEBUG] MainCosts valueArray length: {len(value_array)}")
    
    rate_cards = []
    current_rate_card = None
    header_count = 0
    data_row_count = 0
    
    for item in value_array:
        if item.get('type') != 'object':
            continue
        
        value_object = item.get('valueObject', {})
        
        # Check if this is a header row (has RateName or CostName)
        has_rate_name = 'RateName' in value_object and extract_value(value_object.get('RateName'))
        has_cost_name = 'CostName' in value_object and extract_value(value_object.get('CostName'))
        
        if has_rate_name or has_cost_name:
            header_count += 1
            # This is a header row - start a new rate card
            if current_rate_card and current_rate_card.get('pricing'):
                rate_cards.append(current_rate_card)
            
            current_rate_card = {
                'service_type': extract_value(value_object.get('RateName')),
                'cost_category': extract_value(value_object.get('CostName')),
                'weight_unit': extract_value(value_object.get('Weight')),
                'zone_headers': {},
                'pricing': []
            }
            
            # Extract zone headers
            for key, value in value_object.items():
                if key.startswith('Zone'):
                    zone_name = extract_value(value)
                    if zone_name:
                        current_rate_card['zone_headers'][key] = zone_name
        
        else:
            # This is a data row
            data_row_count += 1
            if current_rate_card:
                weight = extract_value(value_object.get('Weight'))
                if weight:
                    price_row = {
                        'weight': weight,
                        'zone_prices': {}
                    }
                    
                    # Extract zone prices
                    for key, value in value_object.items():
                        if key.startswith('Zone'):
                            price = extract_value(value)
                            if price:
                                price_row['zone_prices'][key] = price
                    
                    if price_row['zone_prices']:
                        current_rate_card['pricing'].append(price_row)
    
    # Don't forget the last rate card
    if current_rate_card and current_rate_card.get('pricing'):
        rate_cards.append(current_rate_card)
    
    print(f"[OK] Processed {len(rate_cards)} rate card sections")
    print(f"    [DEBUG] Header rows: {header_count}, Data rows: {data_row_count}")
    for i, rc in enumerate(rate_cards[:5], 1):
        svc = (rc.get('service_type') or '(none)')[:35]
        cat = (rc.get('cost_category') or '')[:40]
        cat_suffix = '...' if len(rc.get('cost_category') or '') > 40 else ''
        nprice = len(rc.get('pricing', []))
        nzones = len(rc.get('zone_headers', {}))
        print(f"    [DEBUG]   Section {i}: service={svc!r}, category={cat!r}{cat_suffix}, pricing_rows={nprice}, zones={nzones}")
    if len(rate_cards) > 5:
        print(f"    [DEBUG]   ... and {len(rate_cards) - 5} more sections")
    return rate_cards


def process_array_field(array_field, field_name):
    """Process a generic array field"""
    if not array_field or array_field.get('type') != 'array':
        print(f"[WARN] {field_name} is not an array or is empty")
        return []
    
    value_array = array_field.get('valueArray', [])
    if not value_array:
        print(f"[WARN] {field_name} valueArray is empty")
        return []
    
    print(f"    [DEBUG] {field_name}: valueArray length={len(value_array)}")
    results = []
    for item in value_array:
        if item.get('type') != 'object':
            continue
        
        value_object = item.get('valueObject', {})
        row = {}
        
        # Extract all fields from the object
        for key, value in value_object.items():
            extracted = extract_value(value)
            if extracted:
                row[key] = extracted
        
        if row:
            results.append(row)
    
    if results:
        sample_keys = list(results[0].keys())
        print(f"    [DEBUG] {field_name} sample columns: {sample_keys[:12]}{'...' if len(sample_keys) > 12 else ''}")
    return results


def transform_data(fields, client_name):
    """Transform extracted fields into clean structure"""
    print("[*] Transforming data...")
    
    output = {
        'metadata': {
            'client': client_name,
            'carrier': extract_value(fields.get('Carrier')),
            'validity_date': extract_value(fields.get('Validity')),
            'extraction_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'extraction_source': 'Azure Document Intelligence API'
        },
        'MainCosts': [],
        'AddedRates': [],
        'AdditionalCostsPart1': [],
        'CountryZoning': [],
        'AdditionalZoning': [],
        'ZoningMatrix': [],
        'AdditionalCostsPart2': []
    }
    
    # Process MainCosts (special processing with headers and pricing)
    main_costs = fields.get('MainCosts')
    if main_costs:
        output['MainCosts'] = process_main_costs(main_costs)
    else:
        print("[WARN] No MainCosts found in fields")
    
    print(f"    [DEBUG] Metadata: client={output['metadata']['client']!r}, carrier={str(output['metadata']['carrier'])[:40]!r}..., validity={output['metadata']['validity_date']!r}")
    
    # Process all other array fields
    field_names = ['AddedRates', 'AdditionalCostsPart1', 'CountryZoning', 
                   'AdditionalZoning', 'ZoningMatrix', 'AdditionalCostsPart2']
    
    for field_name in field_names:
        field = fields.get(field_name)
        if field:
            output[field_name] = process_array_field(field, field_name)
            print(f"[OK] Processed {field_name}: {len(output[field_name])} items")
        else:
            print(f"[WARN] No {field_name} found in fields")
    
    # Add statistics
    total_main_costs_rows = sum(len(rc.get('pricing', [])) for rc in output['MainCosts'])
    output['statistics'] = {
        'MainCosts_sections': len(output['MainCosts']),
        'MainCosts_rows': total_main_costs_rows,
        'AddedRates_rows': len(output['AddedRates']),
        'AdditionalCostsPart1_rows': len(output['AdditionalCostsPart1']),
        'CountryZoning_rows': len(output['CountryZoning']),
        'AdditionalZoning_rows': len(output['AdditionalZoning']),
        'ZoningMatrix_rows': len(output['ZoningMatrix']),
        'AdditionalCostsPart2_rows': len(output['AdditionalCostsPart2'])
    }
    
    print(f"[OK] Transformation complete")
    print(f"  - MainCosts sections: {output['statistics']['MainCosts_sections']}")
    print(f"  - MainCosts rows: {output['statistics']['MainCosts_rows']}")
    print(f"  - AddedRates: {output['statistics']['AddedRates_rows']} rows")
    print(f"  - AdditionalCostsPart1: {output['statistics']['AdditionalCostsPart1_rows']} rows")
    print(f"  - CountryZoning: {output['statistics']['CountryZoning_rows']} rows")
    print(f"  - AdditionalZoning: {output['statistics']['AdditionalZoning_rows']} rows")
    print(f"  - ZoningMatrix: {output['statistics']['ZoningMatrix_rows']} rows")
    print(f"  - AdditionalCostsPart2: {output['statistics']['AdditionalCostsPart2_rows']} rows")
    
    return output


def save_output(data, output_path):
    """Save transformed data to JSON file"""
    print(f"[*] Saving output to: {output_path}")
    
    try:
        # Create directory if it doesn't exist (though it should already exist)
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        
        # Save with pretty formatting
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        # Get file size
        file_size = os.path.getsize(output_path)
        file_size_kb = file_size / 1024
        
        print(f"[OK] Successfully saved output file")
        print(f"  - File size: {file_size_kb:.2f} KB")
        if 'statistics' in data:
            st = data['statistics']
            total_rows = (st.get('MainCosts_rows', 0) + st.get('AddedRates_rows', 0) +
                         st.get('AdditionalCostsPart1_rows', 0) + st.get('CountryZoning_rows', 0) +
                         st.get('ZoningMatrix_rows', 0))
            print(f"  - [DEBUG] Total data rows written: {total_rows:,}")
        
    except Exception as e:
        print(f"[ERROR] Saving output: {e}")
        raise


def read_client_list(filepath):
    """Read list of client names from file (one per non-empty line)"""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            names = [line.strip() for line in f if line.strip()]
        print(f"    [DEBUG] Client list: {names}")
        return names
    except FileNotFoundError:
        print(f"[WARN] Client file not found: {filepath}")
        return []


def detect_client_from_json(data, client_list):
    """
    Find which client from the list appears in the document.
    Searches analyzeResult.content (full document text).
    Tries longer names first to avoid partial matches (e.g. 'DHL' inside 'DHL LLP AIRBUS').
    Returns first match found, or fallback if none.
    """
    if not client_list:
        print("[WARN] Client list is empty, using 'Unknown'")
        return "Unknown"
    
    content = data.get('analyzeResult', {}).get('content', '')
    if not content:
        print("[WARN] No content in JSON to search for client, using first from list")
        return client_list[0]
    
    print(f"    [DEBUG] Searching content ({len(content):,} chars) for client name...")
    # Sort by length descending: try longer names first to avoid partial matches
    sorted_names = sorted(client_list, key=len, reverse=True)
    print(f"    [DEBUG] Check order (longest first): {[n[:20] + ('...' if len(n) > 20 else '') for n in sorted_names]}")
    
    content_lower = content.lower()
    for name in sorted_names:
        if name.lower() in content_lower:
            idx = content_lower.find(name.lower())
            snippet = content[max(0, idx - 15):idx + len(name) + 15].replace('\n', ' ')
            print(f"[OK] Client detected in document: {name}")
            print(f"    [DEBUG] First occurrence context: ...{snippet}...")
            return name
    
    print("[WARN] No client from list found in document, using first from list")
    return client_list[0]


INPUT_DIR = Path('input')
DEFAULT_INPUT = 'input/converted.json'


def list_input_json_files():
    """Return list of .json files in input/ directory, sorted by name."""
    if not INPUT_DIR.is_dir():
        return []
    return sorted(INPUT_DIR.glob('*.json'), key=lambda p: p.name.lower())


def choose_input_file_interactive():
    """Show a numbered menu of JSON files in input/ and return the selected path."""
    files = list_input_json_files()
    if not files:
        print("[WARN] No JSON files found in input/. Using default.")
        return DEFAULT_INPUT

    print("Select input file to process:")
    print()
    for i, path in enumerate(files, 1):
        size_mb = path.stat().st_size / (1024 * 1024)
        print(f"  {i}. {path.name}  ({size_mb:.2f} MB)")
    print(f"  0. Default ({Path(DEFAULT_INPUT).name})")
    print()

    while True:
        try:
            choice = input("Enter number (0–{}): ".format(len(files))).strip()
            n = int(choice)
            if n == 0:
                return DEFAULT_INPUT
            if 1 <= n <= len(files):
                return str(files[n - 1])
        except ValueError:
            pass
        print("Invalid choice. Enter a number from the list.")


def parse_args():
    """Parse command-line arguments or run interactive input file selection."""
    parser = argparse.ArgumentParser(
        description='Extract structured data from Azure Document Intelligence JSON.'
    )
    parser.add_argument(
        'input_file',
        nargs='?',
        default=None,
        help='Input JSON file path. If omitted, a menu of files in input/ is shown.'
    )
    args = parser.parse_args()
    if args.input_file is not None:
        p = Path(args.input_file)
        if not p.is_absolute() and len(p.parts) == 1:
            resolved = INPUT_DIR / p
            return str(resolved)
        return str(p)
    return choose_input_file_interactive()


def main():
    """Main execution function"""
    print("=" * 60)
    print("DHL RATE CARD DATA EXTRACTOR")
    print("=" * 60)
    print()
    
    # Define paths (input can be overridden by command line)
    input_file = parse_args()
    output_file = 'processing/extracted_data.json'
    client_file = 'addition/clients.txt'
    
    print(f"[*] Input file: {input_file}")
    print()
    
    try:
        # Step 1: Read client list from file
        print("Step 1: Reading client list...")
        client_list = read_client_list(client_file)
        print(f"[OK] Loaded {len(client_list)} client name(s) from list")
        print()
        
        # Step 2: Load input JSON
        print("Step 2: Loading input file...")
        input_data = read_converted_json(input_file)
        print()
        
        # Step 3: Detect client by finding which name from list appears in the document
        print("Step 3: Detecting client from document content...")
        client_name = detect_client_from_json(input_data, client_list)
        print(f"[OK] Client: {client_name}")
        print()
        
        # Step 4: Extract fields
        print("Step 4: Extracting structured fields...")
        fields = extract_fields(input_data)
        print()
        
        # Step 5: Transform data
        print("Step 5: Processing and transforming data...")
        processed_data = transform_data(fields, client_name)
        print()
        
        # Step 6: Save output
        print("Step 6: Saving results...")
        save_output(processed_data, output_file)
        print()
        
        # Success summary
        print("=" * 60)
        print("[SUCCESS] EXTRACTION COMPLETE")
        print("=" * 60)
        print(f"Client: {client_name}")
        print(f"Output: {output_file}")
        print(f"\nExtracted Data:")
        print(f"  - MainCosts: {processed_data['statistics']['MainCosts_sections']} sections, {processed_data['statistics']['MainCosts_rows']} rows")
        print(f"  - AddedRates: {processed_data['statistics']['AddedRates_rows']} rows")
        print(f"  - AdditionalCostsPart1: {processed_data['statistics']['AdditionalCostsPart1_rows']} rows")
        print(f"  - CountryZoning: {processed_data['statistics']['CountryZoning_rows']} rows")
        print(f"  - AdditionalZoning: {processed_data['statistics']['AdditionalZoning_rows']} rows")
        print(f"  - ZoningMatrix: {processed_data['statistics']['ZoningMatrix_rows']} rows")
        print(f"  - AdditionalCostsPart2: {processed_data['statistics']['AdditionalCostsPart2_rows']} rows")
        print()
        print("[DEBUG] Extraction summary:")
        print(f"  - Input: {input_file}")
        print(f"  - Output: {output_file}")
        print(f"  - Client source: detected from document content (list: {client_file})")
        print(f"  - Fields used: analyzeResult.documents[0].fields")
        print()
        
    except Exception as e:
        print()
        print("=" * 60)
        print("[FAILED] EXTRACTION FAILED")
        print("=" * 60)
        print(f"Error: {e}")
        print()
        raise


if __name__ == "__main__":
    main()
