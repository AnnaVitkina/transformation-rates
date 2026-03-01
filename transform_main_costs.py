"""
MainCosts data transformation for the DHL rate-card pipeline.

This module contains all the logic for converting the raw MainCosts JSON data
into the structured rows that get written to the MainCosts Excel tab.

The MainCosts tab is the most complex tab in the workbook.  It shows one row per
"lane" (a unique combination of service type + zone), with all cost categories
(Envelope, Documents, Parcels …) merged into a single wide row.

Functions (public):
  pivot_main_costs                  – legacy flat pivot (kept for reference, not used in main flow)
  build_matrix_main_costs           – builds the lane-based matrix view (main flow)
  expand_main_costs_lanes_by_zoning – replaces letter zones with real O/D pairs

Helper functions (private, prefixed with _):
  _zone_has_letters
  _zone_sort_key
  global_country
  parse_zoning_matrix
  _matrix_zone_to_letter
  _main_words
  _find_matrix_for_service
"""

import re


# ---------------------------------------------------------------------------
# Weight sorting helper
# ---------------------------------------------------------------------------

def _weight_sort_key(w):
    """
    Sort key for weight breakpoint values so they always appear in correct
    numeric order regardless of how they were stored as strings.

    Numeric values (e.g. "0.5", "1", "10.0") are sorted as floats:
        0.5 → 1.0 → 1.5 → 2.0 → 10.0 → 11.0  (correct)
    Non-numeric values (rare edge cases) are sorted alphabetically after
    all numeric values.

    Examples:
        sorted(["10.0", "2.0", "0.5", "1.0"], key=_weight_sort_key)
        → ["0.5", "1.0", "2.0", "10.0"]
    """
    try:
        return (0, float(w))   # numeric: sort by float value
    except (ValueError, TypeError):
        return (1, str(w))     # non-numeric: sort alphabetically after numbers


# ---------------------------------------------------------------------------
# Zone-name helpers
# ---------------------------------------------------------------------------

def _zone_has_letters(zone_name):
    """
    Check whether a zone name uses a letter identifier (e.g. "Zone A") rather than
    a number identifier (e.g. "Zone 1").

    Returns True for "Zone A", "Zone E", etc.
    Returns False for "Zone 1", "Zone 12", etc.
    """
    s = (zone_name or '').strip()
    if not s:
        return False
    if s.upper().startswith('ZONE '):
        suffix = s[5:].strip()
    else:
        suffix = s
    return any(c.isalpha() for c in suffix)


def _zone_is_single_letter(zone_name):
    """
    Return True only when the zone identifier is exactly one letter (e.g. "Zone A", "B").

    This is the fallback criterion used when no matching ZoningMatrix exists for a service.
    A single-letter zone almost certainly refers to a matrix lookup code even when the
    matrix name doesn't match the service name closely enough to be found automatically.

    Examples:
      "Zone A"  -> True   (single letter after "Zone ")
      "Zone AB" -> False  (two letters – probably a real zone name, not a matrix code)
      "Zone 1"  -> False  (number, not a letter)
      "A"       -> True   (bare single letter)
    """
    s = (zone_name or '').strip()
    if not s:
        return False
    if s.upper().startswith('ZONE '):
        suffix = s[5:].strip()
    else:
        suffix = s
    # Exactly one alphabetic character and nothing else
    return len(suffix) == 1 and suffix.isalpha()


def _zone_needs_matrix_lookup(zone_name, service_type, zoning_lookup):
    """
    Decide whether a zone in a given service should be treated as a matrix lookup code
    (i.e. needs to be expanded into real Origin/Destination pairs via the ZoningMatrix).

    NEW TWO-STEP LOGIC:

    Step 1 – Service-matrix match (primary):
      Try to find a ZoningMatrix whose name corresponds to this service type.
      If a match is found, ALL zones for this service are matrix zones – regardless
      of whether their name contains letters or numbers.
      This handles the common case where service "DHL EXPRESS WORLDWIDE THIRD COUNTRY"
      has a matching matrix "DHL EXPRESS THIRD COUNTRY ZONE MATRIX".

    Step 2 – Single-letter fallback:
      If no matrix was found for this service, check whether the zone identifier is
      exactly one letter (e.g. "A", "B", "E").  A bare single letter almost certainly
      means the zone is a matrix lookup code even when the matrix name couldn't be
      matched automatically.

    Returns True if the zone should be flagged as a matrix zone, False otherwise.
    """
    if not zone_name:
        return False

    # Step 1: does a matrix exist for this service?
    if zoning_lookup and _find_matrix_for_service(zoning_lookup, service_type):
        # A matching matrix was found – this zone belongs to it
        return True

    # Step 2: no matrix found for the service; fall back to single-letter check
    return _zone_is_single_letter(zone_name)


def _zone_sort_key(zone_name):
    """
    Generate a sort key for a zone name so that zones appear in a sensible order:
    numeric zones first (Zone 1, Zone 2, Zone 10 …) then letter/other zones after.

    Without this, alphabetical sorting would give: Zone 1, Zone 10, Zone 2 (wrong).
    With this, we get: Zone 1, Zone 2, Zone 10, Zone A (correct).

    Returns a tuple (group, value) where:
      group=0 means numeric zone (sorted by number)
      group=1 means letter/other zone (sorted after all numeric zones)
    """
    s = (zone_name or '').strip()
    if not s:
        return (1, 0)
    if s.upper().startswith('ZONE '):
        suffix = s[5:].strip()
    else:
        suffix = s
    try:
        return (0, float(suffix))
    except (ValueError, TypeError):
        return (1, suffix)   # sort non-numeric zones alphabetically within group 1


def global_country(metadata):
    """
    Extract the country name from the carrier string in the metadata.

    DHL carrier names follow the pattern "DHL Express <Country>" (case-insensitive),
    e.g. "DHL Express France"  -> "France"
         "DHL EXPRESS GERMANY" -> "Germany"
         "DHL express Netherlands" -> "Netherlands"

    The country is everything that comes after the words "DHL" and "EXPRESS"
    (or "EXPRESS" alone), title-cased for consistency.

    If the pattern is not found, the last word of the carrier string is used
    as a fallback so the field is never left empty when a carrier is present.

    This country name is used to fill in the Origin or Destination column for:
    - Domestic lanes (both Origin and Destination = carrier's country)
    - Non-zoned export lanes (Destination = carrier's country)
    - Non-zoned import lanes (Origin = carrier's country)
    """
    import re
    carrier = (metadata.get('carrier') or '').replace('\n', ' ').strip()
    if not carrier:
        return ''

    # Words that signal the end of the country name (non-country suffixes)
    _STOP_WORDS = {
        'customer', 'customers', 'services', 'service', 'surcharges', 'surcharge',
        'export', 'import', 'domestic', 'rates', 'rate', 'ratecard', 'tariff',
        'tariffs', 'zone', 'zones', 'express', 'dhl', 'international', 'standard',
        'priority', 'economy', 'freight', 'air', 'ground', 'parcel', 'and',
    }

    # Match everything after "DHL EXPRESS", then walk word by word until a stop word
    m = re.search(r'\bDHL\s+EXPRESS?\s+(.+)', carrier, re.IGNORECASE)
    if m:
        remainder = m.group(1).strip()
        country_words = []
        for word in remainder.split():
            if word.lower() in _STOP_WORDS:
                break
            country_words.append(word)
        if country_words:
            return ' '.join(country_words).title()   # e.g. "UNITED KINGDOM" -> "United Kingdom"

    # Fallback: return the last word of the carrier string
    parts = carrier.split()
    return parts[-1].title() if parts else ''


# ---------------------------------------------------------------------------
# ZoningMatrix parsing and lane expansion
# ---------------------------------------------------------------------------

def parse_zoning_matrix(zoning_matrix):
    """
    Read the ZoningMatrix data and build a lookup table that answers the question:
    "For zone letter A in matrix X, which (origin zone, destination zone) pairs exist?"

    BACKGROUND – what is a ZoningMatrix?
    The ZoningMatrix is a grid that maps pairs of origin and destination zone numbers
    to a single letter (A, B, C …).  For example:
        Origin 1 -> Destination 3 -> letter "A"
        Origin 2 -> Destination 3 -> letter "A"
        Origin 1 -> Destination 5 -> letter "E"

    The MainCosts pricing table uses those letters as shorthand: instead of listing
    a price for every individual origin/destination pair, it lists one price per letter.
    This function reverses the matrix so we can later expand each letter back into
    all the concrete (origin, destination) pairs it represents.

    THE JSON STRUCTURE:
    The ZoningMatrix arrives as a flat list of rows.  Two types of rows alternate:
      - Header row: has 'MatrixName' filled in + DestinationZone1, DestinationZone2 …
                    whose values are the destination zone numbers (1, 2, 3 …)
      - Data row:   has 'OriginZone' filled in + DestinationZone1, DestinationZone2 …
                    whose values are the zone letters (A, B, E …)

    WHAT THIS FUNCTION RETURNS:
    A dictionary where:
      key   = (matrix_name, zone_letter)   e.g. ("DHL EXPRESS WW ZONE MATRIX", "A")
      value = list of (origin_zone, destination_zone) pairs  e.g. [("1", "3"), ("2", "3")]
    """
    result = {}                    # the lookup table we are building
    dest_cols = None               # ordered list of "DestinationZone1", "DestinationZone2" … keys
    header_dest_nums = None        # the actual destination zone numbers read from the header row
    current_matrix_name = None     # name of the matrix block we are currently inside

    for row in zoning_matrix or []:
        matrix_name = (row.get('MatrixName') or '').strip()
        origin_zone = (row.get('OriginZone') or '').strip()

        if matrix_name:
            # ---------------------------------------------------------------
            # This is a HEADER ROW – it starts a new matrix block.
            # Example: MatrixName="DHL EXPRESS WW ZONE MATRIX",
            #          DestinationZone1="1", DestinationZone2="2", DestinationZone3="3"
            # ---------------------------------------------------------------
            current_matrix_name = matrix_name

            # Find all keys that look like "DestinationZone1", "DestinationZone2" etc.
            # and sort them numerically so column order is preserved.
            dest_keys = sorted(
                [k for k in row if re.match(r'^DestinationZone\d+$', k)],
                key=lambda k: int(re.search(r'\d+', k).group())
            )
            dest_cols = dest_keys

            # Read the actual destination zone numbers from the header cells.
            # e.g. DestinationZone1 -> "1", DestinationZone2 -> "2"
            header_dest_nums = [str(row.get(k, '')).strip() for k in dest_cols]
            continue   # move on to the next row (this header row has no prices)

        if current_matrix_name and origin_zone and dest_cols:
            # ---------------------------------------------------------------
            # This is a DATA ROW – it belongs to the current matrix block.
            # Example: OriginZone="1",
            #          DestinationZone1="A", DestinationZone2="A", DestinationZone3="E"
            # This means: origin 1 -> destination 1 = letter A
            #             origin 1 -> destination 2 = letter A
            #             origin 1 -> destination 3 = letter E
            # ---------------------------------------------------------------
            for col_idx, dest_key in enumerate(dest_cols):
                if col_idx >= len(header_dest_nums):
                    continue   # safety check: don't go past the number of header columns
                dest_zone_num = header_dest_nums[col_idx]   # e.g. "3"
                if not dest_zone_num:
                    continue   # skip if the header had no zone number for this column
                cell_letter = (row.get(dest_key) or '').strip()   # e.g. "A"
                if not cell_letter:
                    continue   # skip empty cells (no zone letter assigned)

                # Build the lookup key: (matrix_name, letter)
                key = (current_matrix_name, cell_letter.upper())
                if key not in result:
                    result[key] = []   # create a new list for this letter if first time seen
                # Record that this (origin, destination) pair maps to this letter
                result[key].append((origin_zone, dest_zone_num))

    return result


def _matrix_zone_to_letter(matrix_zone):
    """
    Extract just the letter part from a zone name like "Zone E" -> "E".
    This is needed because the lookup table is keyed by the letter alone, not the full name.
    If the input is already just a letter (no "Zone " prefix), it is returned as-is in uppercase.
    """
    s = (matrix_zone or '').strip()
    if not s:
        return ''
    if s.upper().startswith('ZONE '):
        return s[5:].strip().upper()   # remove "Zone " and return the rest in uppercase
    return s.upper()


def _main_words(text):
    """
    Split a text string into its meaningful words (all uppercase), ignoring the
    generic words "ZONE" and "MATRIX" which appear in almost every matrix name
    and would cause false matches.

    Example: "DHL EXPRESS THIRD COUNTRY ZONE MATRIX" -> {"DHL", "EXPRESS", "THIRD", "COUNTRY"}
    """
    if not text:
        return set()
    words = set((text or '').upper().split())
    words.discard('ZONE')     # too generic to be useful for matching
    words.discard('MATRIX')   # too generic to be useful for matching
    return words


def _find_matrix_for_service(zoning_lookup, service):
    """
    Given a service type name (e.g. "DHL EXPRESS THIRD COUNTRY"), find which matrix
    in the zoning_lookup corresponds to it.

    WHY THIS IS NEEDED:
    The service names in MainCosts and the matrix names in ZoningMatrix are written
    slightly differently.  For example:
      - Service:  "DHL EXPRESS THIRD COUNTRY"
      - Matrix:   "DHL EXPRESS THIRD COUNTRY ZONE MATRIX"
    We need to match them up despite these differences.

    MATCHING STRATEGY (tries each approach in order, returns the first match found):
      1. Direct substring: does the service name appear inside the matrix name, or vice versa?
      2. Strip " ZONE MATRIX" from the matrix name, then try substring again.
      3. Word-level match: do all meaningful words from the matrix name appear in the service?
         e.g. {"DHL", "EXPRESS", "THIRD", "COUNTRY"} are all present in "DHL EXPRESS THIRD COUNTRY"

    Returns the matching matrix name, or None if no match is found.
    """
    service = (service or '').strip()
    if not service:
        return None
    service_words = _main_words(service)

    # Get all unique matrix names from the lookup (ignoring the zone letter part of each key)
    matrix_names = {mn for (mn, _) in zoning_lookup}

    # --- Attempt 1: direct substring match ---
    for mn in matrix_names:
        if service in mn or mn in service:
            return mn   # found a match, return immediately

    # --- Attempt 2: strip the " ZONE MATRIX" boilerplate and try again ---
    for mn in matrix_names:
        normalized = mn.replace(' ZONE MATRIX', '').strip()
        if service in normalized or normalized in service:
            return mn

    # --- Attempt 3: all meaningful words from the matrix name must be in the service ---
    # This handles cases where word order differs or extra words are present
    for mn in matrix_names:
        matrix_words = _main_words(mn.replace(' ZONE MATRIX', ''))
        # "<=" on sets means "is a subset of": all matrix words appear in service words
        if matrix_words and matrix_words <= service_words:
            return mn

    return None   # no match found in any of the three attempts


# ---------------------------------------------------------------------------
# MainCosts – legacy flat pivot (zones as rows, weights as columns)
# ---------------------------------------------------------------------------

def pivot_main_costs(main_costs, metadata):
    """
    (Legacy / unused view) Convert the MainCosts pricing data into a simple flat table
    where each row = one delivery zone, and each column = one weight bracket.

    Example of what the output looks like:
        Zone    | 0.5 KG | 1 KG | 2 KG
        Zone 1  |  12.50 | 15.00| 18.00
        Zone 2  |  14.00 | 17.50| 21.00

    This is an older, simpler view.  The main view used today is build_matrix_main_costs().
    """
    rows = []   # will hold all the output rows we build

    # Pull the three identity fields that appear on every row
    client = (metadata.get('client') or '')
    carrier = (metadata.get('carrier') or '').replace('\n', ' ')  # remove any line breaks
    validity_date = (metadata.get('validity_date') or '')

    # Loop over each "rate card" block in the MainCosts list.
    # Each rate card covers one service type (e.g. "DHL EXPRESS WORLDWIDE EXPORT")
    # and one cost category (e.g. "Documents").
    for section_idx, rate_card in enumerate(main_costs, 1):
        service_type = rate_card.get('service_type') or ''
        cost_category = rate_card.get('cost_category', '')
        weight_unit = rate_card.get('weight_unit', 'KG')

        # zone_headers maps internal short keys (e.g. "Z1") to display names (e.g. "Zone 1")
        zone_headers = rate_card.get('zone_headers', {})

        # pricing is a list where each entry covers one weight breakpoint.
        # Example entry: { "weight": "0.5", "zone_prices": {"Z1": 12.50, "Z2": 14.00} }
        pricing = rate_card.get('pricing', [])

        # ---------------------------------------------------------------
        # Step 1: Reorganise the data from "weight-first" to "zone-first".
        # ---------------------------------------------------------------
        zone_price_matrix = {}   # zone_name -> { weight -> price }
        weights_set = set()      # collect all unique weight values seen

        for price_entry in pricing:
            weight = price_entry.get('weight', '')
            weights_set.add(weight)
            zone_prices = price_entry.get('zone_prices', {})

            for zone_key, price in zone_prices.items():
                zone_name = zone_headers.get(zone_key, zone_key)
                if zone_name not in zone_price_matrix:
                    zone_price_matrix[zone_name] = {}
                zone_price_matrix[zone_name][weight] = price

        # Sort the weight values numerically
        weights_sorted = sorted(weights_set, key=_weight_sort_key)

        # ---------------------------------------------------------------
        # Step 2: Build one output row per zone.
        # ---------------------------------------------------------------
        for zone_name, weight_prices in zone_price_matrix.items():
            row = {
                'Client': client,
                'Carrier': carrier,
                'Validity Date': validity_date,
                'Section': section_idx,
                'Service Type': service_type,
                'Cost Category': cost_category,
                'Weight Unit': weight_unit,
                'Zone': zone_name
            }

            for weight in weights_sorted:
                col_name = f"{weight} {weight_unit}"   # e.g. "0.5 KG"
                row[col_name] = weight_prices.get(weight, '')

            rows.append(row)

    return rows


# ---------------------------------------------------------------------------
def _format_cost_category(raw_name):
    """
    Wrap a raw cost-category name in the standard "Transport cost (...)" label.

    Examples:
        "Documents up to 2.0 KG"  ->  "Transport cost (Documents up to 2.0 KG)"
        "Envelope up to 300 g"    ->  "Transport cost (Envelope up to 300 g)"
        ""                        ->  ""   (empty stays empty)
    """
    raw_name = (raw_name or '').strip()
    if not raw_name:
        return raw_name
    return f"Transport cost ({raw_name})"


# MainCosts – matrix (lane) view builder
# ---------------------------------------------------------------------------

def build_matrix_main_costs(main_costs, metadata, zoning_matrix=None):
    """
    Build the main pricing table (called the "Matrix view") for the MainCosts Excel tab.

    WHAT THE OUTPUT LOOKS LIKE:
    Each output row = one "lane" = one unique combination of service type + zone.
    All cost categories (Envelope, Documents, Parcels …) for the same lane are
    combined into a single row, with prices stored as separate columns per weight.

    Example output row:
        Lane# | Origin | Destination | Service              | Matrix zone | Envelope 0.5KG | Envelope 1KG | Documents 0.5KG …
        1     | France | Zone 1      | DHL EXPRESS EXPORT   |             | 12.50          | 15.00        | 10.00 …

    HOW MATRIX ZONES ARE DETECTED:
    A zone is flagged as a "Matrix zone" (needs expansion via ZoningMatrix) using
    _zone_needs_matrix_lookup(), which applies a two-step rule:
      1. If a ZoningMatrix whose name matches this service exists → ALL zones for
         that service are matrix zones (regardless of whether they contain letters).
      2. If no matching matrix is found → only flag the zone if its identifier is
         exactly one letter (e.g. "A", "B") as a last-resort fallback.

    Parameters:
      main_costs     – list of rate card sections from the extracted JSON
      metadata       – metadata dict (client, carrier, validity_date …)
      zoning_matrix  – raw ZoningMatrix rows (optional; used to pre-build the lookup
                       so matrix-zone detection is accurate before expansion runs)

    Returns two things:
      rows           – the list of lane rows described above
      category_specs – a description of each cost-category column group,
                       used by write_matrix_sheet() to draw the header
    """
    # Build the zoning lookup once up front so _zone_needs_matrix_lookup can use it
    # during PASS 2 to decide which zones need matrix expansion.
    # If no zoning_matrix was passed in, the lookup will be empty and the fallback
    # single-letter rule will apply instead.
    zoning_lookup = parse_zoning_matrix(zoning_matrix) if zoning_matrix else {}

    # =======================================================================
    # PASS 1 – Figure out what columns the header needs.
    # =======================================================================
    category_specs = []   # will hold: [(category_name, weight_unit, [0.5, 1, 2, …]), …]
    seen_categories = {}  # tracks which categories we have already added (for deduplication)

    for rate_card in main_costs:
        cost_category = _format_cost_category(rate_card.get('cost_category') or '')
        weight_unit = rate_card.get('weight_unit') or 'KG'
        pricing = rate_card.get('pricing', [])

        weights_set = set()
        for pe in pricing:
            w = pe.get('weight', '')
            if w:
                weights_set.add(w)

        weights_sorted = sorted(weights_set, key=_weight_sort_key)

        if cost_category not in seen_categories:
            seen_categories[cost_category] = (weight_unit, weights_sorted)
            category_specs.append((cost_category, weight_unit, weights_sorted))
        else:
            existing_unit, existing_weights = seen_categories[cost_category]
            merged = set(existing_weights) | set(weights_sorted)
            merged_sorted = sorted(merged, key=_weight_sort_key)
            seen_categories[cost_category] = (existing_unit, merged_sorted)
            for i, spec in enumerate(category_specs):
                if spec[0] == cost_category:
                    category_specs[i] = (cost_category, existing_unit, merged_sorted)
                    break

    # =======================================================================
    # PASS 2 – Build one row per lane (service + zone combination).
    # =======================================================================
    lane_rows = {}   # (service_type, zone_name) -> row dict

    for rate_card in main_costs:
        service_type = (rate_card.get('service_type') or '').strip()
        cost_category = _format_cost_category(rate_card.get('cost_category') or '')
        zone_headers = rate_card.get('zone_headers', {})
        pricing = rate_card.get('pricing', [])

        service_lower = service_type.lower()
        is_import = 'import' in service_lower
        is_export = 'export' in service_lower

        # Reorganise the pricing list from weight-first to zone-first
        zone_price_matrix = {}
        for price_entry in pricing:
            weight = price_entry.get('weight', '')
            zone_prices = price_entry.get('zone_prices', {})
            for zone_key, price in zone_prices.items():
                zone_name = zone_headers.get(zone_key, zone_key)
                if zone_name not in zone_price_matrix:
                    zone_price_matrix[zone_name] = {}
                zone_price_matrix[zone_name][weight] = price

        for zone_name, weight_prices in zone_price_matrix.items():
            key = (service_type, zone_name)

            if key not in lane_rows:
                origin = zone_name if is_import else ''
                destination = zone_name if is_export else ''
                # Use the two-step rule: service-matrix match first, single-letter fallback second
                needs_lookup = _zone_needs_matrix_lookup(zone_name, service_type, zoning_lookup)
                matrix_zone = zone_name if needs_lookup else ''
                lane_rows[key] = {
                    'Origin': origin,
                    'Destination': destination,
                    'Service': service_type,
                    'Matrix zone': matrix_zone,
                }

            row = lane_rows[key]
            for weight, price in weight_prices.items():
                row[(cost_category, weight)] = price

    # Get the carrier's country name (e.g. "Netherlands") — used to fill Origin/Destination
    # for domestic and non-zoned lanes where the carrier country is the implicit value.
    carrier_last = global_country(metadata)

    # Sort the lanes: first by service name (alphabetical), then by zone (numeric before letter)
    sorted_keys = sorted(lane_rows.keys(), key=lambda k: (k[0], _zone_sort_key(k[1])))

    rows = []
    for lane, key in enumerate(sorted_keys, 1):
        row = lane_rows[key].copy()
        row['Lane #'] = lane

        service = (row.get('Service') or '').strip()
        matrix_zone = (row.get('Matrix zone') or '').strip()

        if service == 'DHL EXPRESS DOMESTIC':
            # Domestic: both sides are the carrier's own country
            if carrier_last:
                row['Origin'] = carrier_last
                row['Destination'] = carrier_last
        elif not matrix_zone:
            # Non-zoned lane: fill whichever side is still empty with the carrier country
            if carrier_last:
                if not (row.get('Origin') or '').strip():
                    row['Origin'] = carrier_last
                if not (row.get('Destination') or '').strip():
                    row['Destination'] = carrier_last

        rows.append(row)

    return rows, category_specs


def apply_zone_labels_to_main_costs(matrix_rows, zone_label_lookup):
    """
    Replace raw zone names in Origin/Destination with meaningful short labels.

    PURPOSE:
    After build_matrix_main_costs() runs, zoned lanes have Origin or Destination
    values like "Zone 8".  This function replaces those with a label that includes
    the service context, e.g. "ECONOMY_EXP_ZONE_8", so the analyst can immediately
    see which zoning scheme the zone belongs to.

    HOW IT WORKS:
    For each lane row:
      1. Check if Origin or Destination looks like a zone (starts with "Zone ").
      2. Extract the zone number (e.g. "Zone 8" -> "8").
      3. Convert the Service name to its short prefix using the same
         _transform_rate_name_to_short() logic used to build the lookup.
      4. Look up (short_prefix, zone_number) in the zone_label_lookup dict.
      5. If found, replace the Origin/Destination value with the label.

    Rows where Origin/Destination is a country name (not a zone) are left unchanged.

    Parameters:
      matrix_rows       – list of lane row dicts from build_matrix_main_costs()
      zone_label_lookup – dict built by build_zone_label_lookup() in transform_other_tabs.py
                          keys: (short_prefix, zone_number), values: label string

    Returns the same list of rows with Origin/Destination values updated in place.
    """
    if not zone_label_lookup or not matrix_rows:
        return matrix_rows

    # Import here to avoid circular imports (transform_other_tabs imports nothing from here)
    from transform_other_tabs import _transform_rate_name_to_short

    _zone_re = re.compile(r'(?i)^zone\s+(.+)$')

    for row in matrix_rows:
        service = (row.get('Service') or '').strip()
        short_prefix = _transform_rate_name_to_short(service)
        if not short_prefix:
            continue

        for field in ('Origin', 'Destination'):
            val = (row.get(field) or '').strip()
            m = _zone_re.match(val)
            if not m:
                continue   # not a zone value — leave unchanged

            zone_number = m.group(1).strip()
            label = zone_label_lookup.get((short_prefix, zone_number))
            if label:
                row[field] = label

    return matrix_rows


def expand_main_costs_lanes_by_zoning(matrix_rows, zoning_matrix):
    """
    Replace abstract letter-zone rows with real Origin/Destination rows.

    PROBLEM THIS SOLVES:
    After build_matrix_main_costs() runs, some lanes have a "Matrix zone" value
    like "Zone A" instead of real origin/destination countries.  "Zone A" is just
    a code that means "all the origin/destination pairs that belong to group A".
    This function looks up those pairs and creates one concrete row per pair.

    EXAMPLE:
    Before expansion:
        Lane | Origin | Destination | Service          | Matrix zone | Price
        1    |        |             | DHL EXPRESS WW   | Zone A      | 12.50

    After expansion (if Zone A covers origin 1->dest 3 and origin 2->dest 3):
        Lane | Origin | Destination | Service          | Matrix zone | Price
        1    | Zone 1 | Zone 3      | DHL EXPRESS WW   | Zone A      | 12.50
        2    | Zone 2 | Zone 3      | DHL EXPRESS WW   | Zone A      | 12.50

    Rows that already have numeric zones (no Matrix zone value) are left unchanged.
    After all expansion is done, Lane numbers are reassigned from 1 upward.
    """
    if not matrix_rows:
        return matrix_rows

    # Build the full (matrix_name, zone_letter) -> [(origin, dest), ...] lookup
    zoning_lookup = parse_zoning_matrix(zoning_matrix)
    if not zoning_lookup:
        return matrix_rows

    expanded = []

    for row in matrix_rows:
        matrix_zone = (row.get('Matrix zone') or '').strip()
        service = (row.get('Service') or '').strip()

        if not matrix_zone:
            # No letter zone: copy through unchanged
            expanded.append(row)
            continue

        zone_letter = _matrix_zone_to_letter(matrix_zone)
        if not zone_letter:
            expanded.append(row)
            continue

        matrix_name = _find_matrix_for_service(zoning_lookup, service)
        if not matrix_name:
            expanded.append(row)
            continue

        key = (matrix_name, zone_letter)
        pairs = zoning_lookup.get(key, [])
        if not pairs:
            expanded.append(row)
            continue

        # Create one copy of the row per (origin, destination) pair
        for origin_zone, dest_zone in pairs:
            new_row = row.copy()
            new_row['Origin'] = f"Zone {origin_zone}" if origin_zone else ''
            new_row['Destination'] = f"Zone {dest_zone}" if dest_zone else ''
            expanded.append(new_row)

    # Reassign Lane # sequentially after expansion
    for lane, row in enumerate(expanded, 1):
        row['Lane #'] = lane

    return expanded
