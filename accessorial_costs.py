"""
Accessorial Costs tab builder for the DHL rate-card workbook.

This module handles everything related to the "Accessorial Costs" Excel tab:
  - Loading the reference list of approved cost type names from a client file
  - Fuzzy-matching each raw cost name against that approved list
  - Building the final rows for the tab

Public functions:
  build_accessorial_costs_rows  – main entry point; returns (rows, ref_file_used)

Private helpers:
  _load_accessorial_cost_type_names  – reads the approved name list from xlsx/csv
  _token_set                         – splits a string into meaningful word tokens
  _best_match_cost_type              – scores and returns the best fuzzy match
"""

import difflib
import re
from pathlib import Path


def _clean_currency_and_price(raw_price, raw_currency):
    """
    Separate the currency code from numeric/text noise in both fields.

    PROBLEM:
    The extracted values often mix numbers and currency codes together, e.g.:
        CostPrice  = "0.50 EUR met een minimum van 24.00 EUR"
        CostCurrency = "0.50 EUR"

    WHAT WE WANT:
        Currency   = "EUR"          (letters only)
        Cost Price = "0.50  met een minimum van 24.00"  (currency code removed)

    STEPS:
    1. Extract the currency code from the raw_currency value — keep only the
       uppercase letter sequence (e.g. "0.50 EUR" -> "EUR").
    2. If a currency code was found, remove every occurrence of it from
       raw_price (case-insensitive) so the price field contains only numbers
       and descriptive text.
    3. Collapse any double spaces left behind and strip the result.

    If no currency code can be extracted, both values are returned unchanged.
    """
    raw_price = str(raw_price or '').strip()
    raw_currency = str(raw_currency or '').strip()

    # Step 1: extract the currency code (3 uppercase letters, e.g. EUR, USD, GBP)
    currency_match = re.search(r'\b([A-Z]{2,4})\b', raw_currency)
    if not currency_match:
        # No recognisable currency code — return as-is
        return raw_price, raw_currency

    currency_code = currency_match.group(1)   # e.g. "EUR"

    # Step 2: remove the currency code from the price string
    cleaned_price = re.sub(r'\b' + re.escape(currency_code) + r'\b', '', raw_price, flags=re.IGNORECASE)
    cleaned_price = re.sub(r'  +', ' ', cleaned_price).strip()

    return cleaned_price, currency_code


def _load_accessorial_cost_type_names(ref_path):
    """
    Read the list of approved/canonical cost type names from a reference file.

    PURPOSE:
    The rate card PDF uses its own names for costs (e.g. "Premium 9:00 Delivery").
    The business wants these mapped to standardised names from an approved list
    (e.g. "9:00 Service Fee").  This function loads that approved list.

    The reference file must have a column called 'Name'.  Supported file formats:
      - Excel (.xlsx or .xls)
      - CSV (.csv)

    Returns a deduplicated list of name strings, in the order they appear in the file.
    Returns an empty list [] if the file doesn't exist or has no 'Name' column.
    """
    ref_path = Path(ref_path)
    if not ref_path.exists():
        return []
    names = []
    try:
        if ref_path.suffix.lower() in ('.xlsx', '.xls'):
            import openpyxl
            wb = openpyxl.load_workbook(ref_path, read_only=True, data_only=True)
            ws = wb.active
            header = None
            name_col = None
            for row in ws.iter_rows(values_only=True):
                if header is None:
                    header = [str(c).strip() if c is not None else '' for c in row]
                    for i, h in enumerate(header):
                        if h == 'Name':
                            name_col = i
                            break
                    if name_col is None:
                        break
                    continue
                if name_col is not None and name_col < len(row):
                    val = row[name_col]
                    if val is not None and str(val).strip():
                        names.append(str(val).strip())
            wb.close()
        elif ref_path.suffix.lower() == '.csv':
            import csv
            with open(ref_path, 'r', encoding='utf-8-sig', newline='') as f:
                reader = csv.reader(f)
                header = next(reader, None)
                if header:
                    try:
                        name_col = header.index('Name')
                    except ValueError:
                        name_col = None
                    if name_col is not None:
                        for row in reader:
                            if name_col < len(row) and row[name_col].strip():
                                names.append(row[name_col].strip())
        else:
            return []
    except Exception:
        return []

    # Remove duplicates while keeping the original order
    return list(dict.fromkeys(names))


def _token_set(text):
    """
    Break a text string into a set of individual words (tokens) in lowercase.
    Also handles time-like tokens such as "9:00" as a single token (not split at the colon).

    Example: "Premium 9:00 Delivery Fee" -> {"premium", "9:00", "delivery", "fee"}

    This is used by the fuzzy matching function to compare cost names word-by-word.
    """
    import re
    s = (text or '').lower().strip()
    tokens = set(re.findall(r'[a-z0-9]+(?::[a-z0-9]+)?|[a-z]+', s))
    return tokens


def _best_match_cost_type(original_name, name_list, cutoff=0.4):
    """
    Find the best matching canonical cost type name for a given original cost name.

    WHY FUZZY MATCHING?
    The cost names in the rate card PDF (e.g. "Premium 9:00:") don't always match
    exactly the standardised names in the reference file (e.g. "9:00 Service Fee").
    We use a scoring system to find the closest match.

    HOW THE SCORE WORKS:
    For each candidate name in the reference list, we compute a combined score:
      score = character_similarity + token_overlap_bonus

      character_similarity: a 0-to-1 score from Python's difflib library that measures
                            how similar two strings look character by character.
                            e.g. "Premium 9:00" vs "9:00 Service Fee" -> ~0.35

      token_overlap_bonus:  an extra bonus (up to 0.4) for shared meaningful words.
                            "Meaningful" means the word is at least 2 characters long
                            or contains ":" (to catch time codes like "9:00").
                            e.g. both contain "9:00" -> bonus = 0.4

    The candidate with the highest combined score wins.
    If the winning score is below 0.3, we return '' (no match good enough).
    """
    if not original_name or not name_list:
        return ''
    original = str(original_name).strip()
    if not original:
        return ''

    orig_tokens = _token_set(original)
    best_score = -1.0
    best_name = ''

    for name in name_list:
        name_str = str(name).strip()
        if not name_str:
            continue

        # Measure character-level similarity (0 = completely different, 1 = identical)
        char_ratio = difflib.SequenceMatcher(None, original.lower(), name_str.lower()).ratio()

        name_tokens = _token_set(name_str)
        shared = orig_tokens & name_tokens

        # Calculate the token overlap bonus.
        # Only count "meaningful" tokens (length >= 2 or contains ':' for time codes).
        meaningful_orig = {t for t in orig_tokens if len(t) >= 2 or ':' in t}
        if meaningful_orig:
            token_bonus = (len(shared & meaningful_orig) / len(meaningful_orig)) * 0.4
        else:
            token_bonus = 0.0

        score = char_ratio + token_bonus

        if score > best_score:
            best_score = score
            best_name = name_str

    return best_name if best_score >= 0.3 else ''


def build_accessorial_costs_rows(additional_costs_1, additional_costs_2, metadata,
                                  cost_type_ref_path=None, accessorial_folder=None):
    """
    Build the rows for the "Accessorial Costs" Excel tab.

    WHAT ARE ACCESSORIAL COSTS?
    These are extra charges on top of the base shipping rate, such as:
    fuel surcharges, remote area fees, signature fees, Saturday delivery fees, etc.
    They come from two sections of the JSON: AdditionalCostsPart1 and AdditionalCostsPart2.

    WHAT THIS FUNCTION DOES:
    1. Converts every item from both parts into a row matching ACCESSORIAL_COSTS_COLUMNS.
    2. Tries to fill the "Cost Type" column by matching each "Original Cost Name" against
       a reference file of approved cost type names for this client.
       - The reference file is found by looking in accessorial_folder for a file whose
         filename contains the client name (e.g. "Acme_Accessorial_Costs.xlsx" for client "Acme").
       - Matching is done by _best_match_cost_type() (fuzzy/approximate matching).
       - If no reference file is found, Cost Type is left blank.

    Returns: (list_of_rows, path_of_reference_file_used_or_None)
    """
    carrier = (metadata.get('carrier') or '').replace('\n', ' ')
    validity_date = (metadata.get('validity_date') or '')

    def item_to_row(item):
        """Convert one JSON cost item into a row dict matching ACCESSORIAL_COSTS_COLUMNS."""
        raw_price    = item.get('CostPrice') or item.get('CostAmount') or ''
        raw_currency = item.get('CostCurrency', '')
        cost_price, currency = _clean_currency_and_price(raw_price, raw_currency)
        return {
            'Original Cost Name': item.get('CostName', ''),
            'Cost Type': '',                                    # filled later by fuzzy matching
            'Cost Price': cost_price,
            'Currency': currency,
            'Rate by': item.get('PriceMechanism', ''),
            'Apply Over': item.get('ApplyTo', ''),
            'Apply if': '',
            'Additional info(Cost Code)': item.get('CostCode', ''),
            'Valid From': validity_date,
            'Valid To': '',
            'Carrier': carrier,
        }

    # Combine AdditionalCostsPart1 and AdditionalCostsPart2 into one flat list
    rows = []
    for item in additional_costs_1 or []:
        rows.append(item_to_row(item))
    for item in additional_costs_2 or []:
        rows.append(item_to_row(item))

    # -----------------------------------------------------------------------
    # Find the reference file for Cost Type fuzzy matching.
    # Search folders in priority order for a file whose name contains the
    # client name (case-insensitive).
    #
    # Search order:
    #   1. The configured accessorial_folder (e.g. Google Drive path)
    #   2. addition/   (local fallback folder, always checked if #1 not found)
    # -----------------------------------------------------------------------
    if cost_type_ref_path is None:
        client = (metadata.get('client') or '').strip()
        ext_order = ('.xlsx', '.xls', '.csv')

        # Build the list of folders to search, skipping ones that don't exist
        search_dirs = []
        if accessorial_folder:
            search_dirs.append(Path(accessorial_folder))
        # Always add addition/ as a local fallback
        local_addition = Path(__file__).resolve().parent / 'addition'
        if local_addition not in search_dirs:
            search_dirs.append(local_addition)

        print("[*] Accessorial Cost Type mapping: searching for client reference file...")
        print(f"    Client: {client or '(none)'}")
        print(f"    Search folders: {[str(d) for d in search_dirs]}")

        if not client:
            print("[*] Accessorial cost mapping: no client in metadata, Cost Type left empty")
        else:
            client_lower = client.lower()
            for search_dir in search_dirs:
                if not search_dir.exists() or not search_dir.is_dir():
                    print(f"    [SKIP] Folder not found: {search_dir}")
                    continue
                candidates = [
                    p for p in search_dir.iterdir()
                    if p.is_file()
                    and p.suffix.lower() in ext_order
                    and client_lower in p.stem.lower()
                ]
                if candidates:
                    cost_type_ref_path = min(
                        candidates,
                        key=lambda p: ext_order.index(p.suffix.lower()) if p.suffix.lower() in ext_order else 99,
                    )
                    print(f"[*] Accessorial cost mapping: found '{cost_type_ref_path.name}' in {search_dir}")
                    break
                else:
                    print(f"    [MISS] No file with client '{client}' in name in {search_dir}")

            if cost_type_ref_path is None:
                print(f"[*] Accessorial cost mapping: no reference file found for client '{client}', Cost Type left empty")

    if cost_type_ref_path:
        name_list = _load_accessorial_cost_type_names(cost_type_ref_path)
        if name_list:
            for row in rows:
                original = row.get('Original Cost Name', '')
                row['Cost Type'] = _best_match_cost_type(original, name_list)
            print(f"[*] Accessorial Cost Type: filled from {cost_type_ref_path.name} ({len(name_list)} cost types, {len(rows)} rows)")
        else:
            print(f"[*] Accessorial Cost Type: file {cost_type_ref_path.name} has no 'Name' column or is empty, Cost Type left blank")

    return rows, cost_type_ref_path
