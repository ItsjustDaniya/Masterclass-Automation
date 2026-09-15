#!/usr/bin/env python3
"""
M330 — Metabase → Google Sheets sync
Unattended GitHub Actions cron version.

Ported from M330_metabase_sync_4.ipynb (Colab notebook, manual-run version)
for scheduled execution. All sync logic (diff-based reconcile: append new
rows / update changed rows / skip unchanged, batched writes, per-card retry
with backoff, per-tab error isolation) is unchanged from the notebook —
only the two Colab-specific auth steps were replaced:

  - Google Sheets auth: `google.colab.auth.authenticate_user()` (interactive
    OAuth) -> a service account (`SERVICE_ACCOUNT_JSON` env var / GitHub
    secret). The sheet must be shared with the service account's email.
  - Config/secrets: `google.colab.userdata.get(...)` -> plain environment
    variables (GitHub Actions secrets).

Any uncaught exception, or any tab failing to sync, exits non-zero so the
GitHub Actions run goes red — but every tab is still attempted even if an
earlier one fails (matches the notebook's per-tab error isolation).
"""

import os
import re
import sys
import json
import time
import logging
import traceback
from datetime import datetime

import requests
import gspread
from dateutil import parser as dateparser
from gspread.exceptions import APIError
from gspread.utils import a1_to_rowcol, rowcol_to_a1
from google.oauth2.service_account import Credentials

start_time = time.time()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("mb-sync")

# ═══════════════════════════════════════════════════════════════════════════
# ENV & AUTH
# ═══════════════════════════════════════════════════════════════════════════
METABASE_API_KEY = os.getenv("METABASE_API_KEY")
service_account_json = os.getenv("SERVICE_ACCOUNT_JSON")

missing = [n for n, v in [
    ("METABASE_API_KEY", METABASE_API_KEY),
    ("SERVICE_ACCOUNT_JSON", service_account_json),
] if not v]
if missing:
    raise ValueError(f"❌ Missing environment variables: {', '.join(missing)}")

service_info = json.loads(service_account_json)
creds = Credentials.from_service_account_info(
    service_info,
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ],
)
gc = gspread.authorize(creds)


def safe_open_by_key(key):
    """gc.open_by_key() wrapped to fail with the exact service-account email
    to share the sheet with, instead of a bare SpreadsheetNotFound."""
    try:
        return gc.open_by_key(key)
    except gspread.exceptions.SpreadsheetNotFound:
        raise RuntimeError(
            f"❌ Could not open Google Sheet with key '{key}'. Share it with "
            f"this service account as Editor: {service_info.get('client_email')}"
        )


def get_env_or_default(name, default=None):
    val = os.getenv(name)
    return val if val else default


METABASE_URL = get_env_or_default(
    "METABASE_URL", "https://metabase-lierhfgoeiwhr.newtonschool.co"
).rstrip("/")

GOOGLE_SHEET_ID = get_env_or_default(
    "GOOGLE_SHEET_ID", "17QTxOXlfY3FLBsaumZhDn0WlNo2xSH1cXXnOf5d-Qk4"
)

# Dashboard 570 ("MC Views") is where several of these questions live as tabs -
# used as a fallback if a question isn't found in the general question list.
METABASE_DASHBOARD_ID = get_env_or_default("METABASE_DASHBOARD_ID", "570")

log.info("🔎 ENV CHECK")
log.info(f"   Metabase API key   : {'[SET]' if METABASE_API_KEY else '[MISSING]'}")
log.info(f"   SA client_email    : {service_info.get('client_email')}")
log.info(f"   METABASE_URL       : {METABASE_URL}")
log.info(f"   GOOGLE_SHEET_ID    : {GOOGLE_SHEET_ID}")
log.info(f"   METABASE_DASHBOARD_ID : {METABASE_DASHBOARD_ID}")

# ═══════════════════════════════════════════════════════════════════════════
# SHEET ↔ METABASE QUESTION MAPPING (unchanged from the notebook)
# ═══════════════════════════════════════════════════════════════════════════
SHEET_CARD_MAP = {
    "Registrations":                   {"question": "MC Registrations --> RFD", "id_col": 0},
    "Viewers":                         {"question": "Class wise viewer breakdown (MC)", "id_col": 0},
    "Movement":                        {"question": "MC viewers moment", "id_col": 0},

    # Direct card_id used here (from the URL you shared: .../question/10807-...)
    # instead of name-based lookup - just as reliable, and skips one API call.
    "RCB":                             {"card_id": 10807, "question": "Class wise MC to RCB ratios", "id_col": 0},

    # Direct card_id used here (from the URL you shared:
    # .../question/10816-rfds-mc?mx_course_enrolled=&lecture_date=) - same
    # pattern as "RCB" above. id_col assumed 0 (Lecture ID first column,
    # matching every other tab here) - if this question's first column isn't
    # the lecture ID, change id_col to match its actual position.
    "MOM RFDs":                        {"card_id": 10816, "question": "RFDs MC", "id_col": 0},

    "Retention":                       {"question": "Class wise viewer retention", "id_col": 0},
    "First 15 min":                    {"question": "First 15 min of class breakdown", "id_col": 0},
    "Time Spent":                      {"question": "MC_lecture_timespent", "id_col": 0},
    "Ratings":                         {"question": "MC ratings", "id_col": 0},

    # This tab has a live formula in column E ("Class label" = XLOOKUP into
    # Class Labels), so we must NOT append a plain contiguous row across it -
    # that would overwrite the formula with a static value. `data_columns`
    # tells the sync where the real data columns are, skipping E. Build the
    # Metabase question to return exactly these columns, in this order
    # (25 columns: A-D, then F-Z - no "Class label" column in the query at all).
    # Direct card_id used here (from the URL you shared: .../question/11553-...)
    "RFD - Class wise - Viewer breakdown to RFD": {
        "card_id": 11553,
        "question": "RFD - Class wise viewer breakdown to RFD",
        "id_col": 0,   # Lecture ID (column A)
        "data_columns": [
            "A", "B", "C", "D",
            "F", "G", "H", "I", "J", "K", "L", "M", "N", "O",
            "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z",
        ],
    },
}

# ═══════════════════════════════════════════════════════════════════════════
# SYNC FUNCTIONS (unchanged from the notebook)
# ═══════════════════════════════════════════════════════════════════════════
MAX_RETRIES = 5
RETRY_BACKOFF_SECONDS = 5
BATCH_PAUSE_SECONDS = 1.2


def metabase_headers():
    return {"x-api-key": METABASE_API_KEY, "Content-Type": "application/json"}


def metabase_request(method, path, timeout=60, max_retries=3, **kwargs):
    url = f"{METABASE_URL}{path}"

    resp = None
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.request(method, url, headers=metabase_headers(), timeout=timeout, **kwargs)
            break
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            if attempt == max_retries:
                raise RuntimeError(
                    f"Metabase request timed out after {max_retries} attempts "
                    f"({timeout}s timeout each): {method} {path}\n"
                    f"Last error: {e}\n"
                    "This question may just be slow - consider raising `timeout` for it, "
                    "or check if it can be optimized in Metabase."
                ) from e
            wait = 5 * attempt
            log.warning("Metabase request timed out (attempt %d/%d), retrying in %ss...",
                        attempt, max_retries, wait)
            time.sleep(wait)

    if resp.status_code == 401:
        raise RuntimeError(
            "Metabase returned 401 Unauthorized - check METABASE_API_KEY (GitHub secret), "
            "confirm it hasn't been revoked, and that its group can view these questions."
        )
    if not resp.ok:
        # Surface Metabase's actual error body (usually explains *why* the
        # question failed - e.g. a broken query) instead of a generic HTTPError.
        try:
            detail = resp.json()
        except ValueError:
            detail = resp.text
        raise RuntimeError(
            f"Metabase request failed: {method} {path} -> {resp.status_code}\n"
            f"Response: {detail}\n"
            "If this is a /api/card/*/query call, open that question directly in "
            "Metabase (outside the dashboard) and confirm it runs without error there first."
        )
    return resp


def build_question_name_index():
    index = {}
    resp = metabase_request("GET", "/api/card")
    for card in resp.json():
        name = (card.get("name") or "").strip().lower()
        if name and name not in index:
            index[name] = card["id"]
    log.info("Indexed %d Metabase questions from /api/card", len(index))

    if METABASE_DASHBOARD_ID:
        resp = metabase_request("GET", f"/api/dashboard/{METABASE_DASHBOARD_ID}")
        dashcards = resp.json().get("dashcards", [])
        added = 0
        for dc in dashcards:
            card = dc.get("card") or {}
            name = (card.get("name") or "").strip().lower()
            card_id = card.get("id")
            if name and card_id and name not in index:
                index[name] = card_id
                added += 1
        log.info("Indexed %d additional questions from dashboard %s", added, METABASE_DASHBOARD_ID)
    return index


def resolve_card_ids(sheet_card_map):
    # Only build the name index if at least one entry actually needs name lookup.
    needs_lookup = any("card_id" not in cfg for cfg in sheet_card_map.values())
    name_index = build_question_name_index() if needs_lookup else {}

    resolved, missing_qs = {}, []
    for tab_name, cfg in sheet_card_map.items():
        if "card_id" in cfg:
            resolved[tab_name] = cfg["card_id"]
            log.info("Using explicit card_id %s for tab '%s'", cfg["card_id"], tab_name)
            continue
        question_name = cfg["question"]
        card_id = name_index.get(question_name.strip().lower())
        if card_id is None:
            missing_qs.append(f"'{question_name}' (for tab '{tab_name}')")
        else:
            resolved[tab_name] = card_id
            log.info("Resolved question '%s' -> card_id %s", question_name, card_id)
    if missing_qs:
        raise RuntimeError(
            "Could not find these Metabase questions by name: " + "; ".join(missing_qs) +
            ". Check exact spelling/casing, confirm your API key's group can see them, "
            "or set the METABASE_DASHBOARD_ID secret if they only live inside a dashboard."
        )
    return resolved


def _looks_date_like(s):
    '''Cheap pre-filter before spending a dateutil.parse() call: only strings
    with a date-ish separator or a month name, and not bare numbers (which
    would otherwise get misread as dates - e.g. "31" or "3.23").'''
    if not isinstance(s, str):
        return False
    s = s.strip()
    if len(s) < 6:
        return False
    core = s.replace(".", "", 1).replace("-", "", 1)
    if core.isdigit():
        return False
    if not re.search(r"[/:\-]", s) and not re.search(
        r"(?i)jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec", s
    ):
        return False
    return True


def try_parse_date(value):
    '''Returns a datetime if `value` is confidently a date/timestamp string
    (handles Metabase's ISO-8601 output, e.g. 2026-07-30T14:30:00Z), else None.'''
    if not _looks_date_like(value):
        return None
    try:
        return dateparser.parse(value)
    except (ValueError, OverflowError, TypeError):
        return None


def format_for_sheets(dt):
    '''Formats a datetime as human-readable text - no "T", no "Z", no offset.
    Used ONLY for the diff/compare step (values_equal against what's already
    in the sheet) and for logging; the actual write to Sheets uses a real
    serial number instead (date_to_serial) - see the note there for why.'''
    if dt.hour == 0 and dt.minute == 0 and dt.second == 0 and dt.microsecond == 0:
        return dt.strftime("%Y-%m-%d")
    return dt.strftime("%Y-%m-%d %H:%M:%S")


# Google Sheets (and Excel) date serial numbers count days since this epoch.
_SHEETS_EPOCH = datetime(1899, 12, 30)


def date_to_serial(dt):
    '''Converts a (possibly timezone-aware) datetime to a Sheets/Excel serial
    number, keeping its wall-clock fields as-is - same convention as
    format_for_sheets, which also doesn't convert the UTC offset, just drops
    it (Metabase already returns times in the display timezone, e.g. IST).

    Writing this serial number with value_input_option=RAW, together with an
    explicit DATE/DATE_TIME number format (see sync_tab), is what actually
    guarantees the cell becomes a real Sheets date - it removes any
    dependence on Sheets' own string-to-date auto-detection, which is what
    silently left values like "2026-07-30T14:30:00+05:30" sitting in the
    sheet as plain, unconverted text instead of a real date/time.'''
    naive = dt.replace(tzinfo=None)
    delta = naive - _SHEETS_EPOCH
    return delta.days + delta.seconds / 86400 + delta.microseconds / 86400e6


def normalize_dates_in_row(row):
    '''For every date/timestamp-looking cell in `row`, returns both a
    human-readable display string (diffing/logging only) and a Sheets serial
    number (the actual write value - see date_to_serial). Non-date cells
    (IDs, names, numbers) pass through unchanged in both. Also reports, per
    column position, whether that cell held a date and whether it carried a
    real time-of-day component (vs. a bare date at midnight).'''
    display, write, date_flags, has_time = [], [], [], []
    for v in row:
        dt = try_parse_date(v) if isinstance(v, str) else None
        if dt is not None:
            display.append(format_for_sheets(dt))
            write.append(date_to_serial(dt))
            date_flags.append(True)
            has_time.append(not (dt.hour == 0 and dt.minute == 0 and dt.second == 0 and dt.microsecond == 0))
        else:
            display.append(v)
            write.append(v)
            date_flags.append(False)
            has_time.append(False)
    return display, write, date_flags, has_time


def fetch_card_rows(card_id, timeout=180):
    # Some questions (e.g. large joins/funnels) genuinely take a while to run.
    # metabase_request also retries on timeout, so total worst case is timeout*3.
    resp = metabase_request("POST", f"/api/card/{card_id}/query", timeout=timeout)
    payload = resp.json()
    data = payload.get("data", {})
    rows = data.get("rows", [])
    cols = [c.get("display_name") or c.get("name") for c in data.get("cols", [])]
    log.info("Card %s: fetched %d rows, columns=%s", card_id, len(rows), cols)

    display_rows, write_rows = [], []
    date_col_positions, date_col_has_time = set(), set()
    for r in rows:
        display, write, date_flags, has_time = normalize_dates_in_row(r)
        display_rows.append(display)
        write_rows.append(write)
        for i, is_date in enumerate(date_flags):
            if is_date:
                date_col_positions.add(i)
                if has_time[i]:
                    date_col_has_time.add(i)

    # `cols` and the date-column info are returned alongside the rows (not
    # just logged) so sync_tab can write a header row into a brand-new/empty
    # tab, and force a real DATE/DATE_TIME number format on date columns.
    return display_rows, write_rows, cols, date_col_positions, date_col_has_time


def with_retry(fn, *args, **kwargs):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return fn(*args, **kwargs)
        except APIError as e:
            if attempt == MAX_RETRIES:
                raise
            wait = RETRY_BACKOFF_SECONDS * attempt
            log.warning("Sheets API error (%s), retrying in %ss...", e, wait)
            time.sleep(wait)


def normalise_id(value):
    try:
        return str(int(float(value)))
    except (TypeError, ValueError):
        return str(value).strip()


def col_letter_to_idx(letter):
    return a1_to_rowcol(f"{letter}1")[1] - 1


def idx_to_letter(idx):
    a1 = rowcol_to_a1(1, idx + 1)
    return re.match(r"[A-Z]+", a1).group()


def values_equal(a_list, b_list):
    '''Cell-by-cell compare, tolerant of float formatting but NOT of real
    differences - a 0.1 change in a number will correctly register as changed.'''
    if len(a_list) != len(b_list):
        return False
    for a, b in zip(a_list, b_list):
        a_s, b_s = str(a).strip(), str(b).strip()
        if a_s == b_s:
            continue
        try:
            if abs(float(a_s) - float(b_s)) < 1e-9:
                continue
        except (ValueError, TypeError):
            pass
        return False
    return True


def group_contiguous_columns(letters):
    idxs = [col_letter_to_idx(l) for l in letters]
    blocks = []
    cur_letters, cur_positions = [letters[0]], [0]
    for i in range(1, len(letters)):
        if idxs[i] == idxs[i - 1] + 1:
            cur_letters.append(letters[i])
            cur_positions.append(i)
        else:
            blocks.append((cur_letters, cur_positions))
            cur_letters, cur_positions = [letters[i]], [i]
    blocks.append((cur_letters, cur_positions))
    return blocks


def emit_block_writes(letters, block_positions, row_start, row_end,
                       display_rows, write_rows, date_col_positions, date_col_has_time,
                       value_batch, date_batch, fmt_requests, sheet_id):
    '''Queues the writes for one contiguous sheet-column block (`letters`,
    with `block_positions` the matching 0-based positions in each fetched
    row), covering sheet rows row_start..row_end inclusive. Splits the block
    into consecutive date / non-date sub-runs:
      - non-date cells -> `value_batch` (written USER_ENTERED, as text -
        Sheets' own auto-detect is fine for plain numbers/strings)
      - date cells -> `date_batch` (written RAW, as Sheets serial numbers)
        plus a matching entry in `fmt_requests` that explicitly sets that
        range's number format to DATE or DATE_TIME. This combination is what
        actually fixes dates getting stuck as literal text - see
        date_to_serial's docstring for why.
    `display_rows`/`write_rows` must have one entry per sheet row being
    written here (row_end - row_start + 1 of them), in row order.'''
    idx, n = 0, len(block_positions)
    while idx < n:
        run_is_date = block_positions[idx] in date_col_positions
        j = idx
        while j < n and (block_positions[j] in date_col_positions) == run_is_date:
            j += 1
        run_positions = block_positions[idx:j]
        range_name = f"{letters[idx]}{row_start}:{letters[j-1]}{row_end}"
        if run_is_date:
            values = [[wr[p] for p in run_positions] for wr in write_rows]
            date_batch.append({"range": range_name, "values": values})
            is_datetime = any(p in date_col_has_time for p in run_positions)
            fmt_type = "DATE_TIME" if is_datetime else "DATE"
            pattern = "yyyy-mm-dd hh:mm:ss" if is_datetime else "yyyy-mm-dd"
            fmt_requests.append({
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": row_start - 1,
                        "endRowIndex": row_end,
                        "startColumnIndex": col_letter_to_idx(letters[idx]),
                        "endColumnIndex": col_letter_to_idx(letters[j - 1]) + 1,
                    },
                    "cell": {"userEnteredFormat": {"numberFormat": {"type": fmt_type, "pattern": pattern}}},
                    "fields": "userEnteredFormat.numberFormat",
                }
            })
        else:
            values = [[dr[p] for p in run_positions] for dr in display_rows]
            value_batch.append({"range": range_name, "values": values})
        idx = j


def sync_tab(sheet, tab_name, card_id, id_col, data_columns=None, header_row=1, timeout=180):
    '''Fetches fresh Metabase rows and reconciles them against the sheet:
    - a lecture ID not already in the sheet -> appended as a new row
    - a lecture ID already present, with any value different (even a small
      numeric change) -> that row is updated in place
    - a lecture ID present with identical values -> left untouched
    Returns (fresh_rows, new_count, updated_count).'''
    ws = sheet.worksheet(tab_name)
    display_rows, write_rows, headers, date_col_positions, date_col_has_time = fetch_card_rows(
        card_id, timeout=timeout
    )

    all_values = with_retry(ws.get_all_values)

    # A brand-new/empty tab has no header row at all - the logic below treats
    # row 1 as headers and starts reading data from row 2, so without this a
    # first-ever sync would happily write data starting at A1 and the tab
    # would never get headings. Only fires when row 1 is genuinely blank;
    # an existing (already-headed) tab is left exactly as the user set it up.
    header_row_missing = (len(all_values) == 0) or all(not c.strip() for c in all_values[0])
    if header_row_missing and headers:
        header_writes = []
        if data_columns:
            for letters, block_positions in group_contiguous_columns(data_columns):
                header_writes.append({
                    "range": f"{letters[0]}1:{letters[-1]}1",
                    "values": [[headers[p] for p in block_positions]],
                })
        else:
            end_letter = idx_to_letter(len(headers) - 1)
            header_writes.append({"range": f"A1:{end_letter}1", "values": [headers]})
        with_retry(ws.batch_update, header_writes, value_input_option="USER_ENTERED")
        log.info("[%s] tab had no header row - wrote column headings: %s", tab_name, headers)
        # Keep the row-number bookkeeping below consistent with a header
        # row now existing at row 1 (so appends land at row 2, not row 1).
        all_values = [headers] if not all_values else [headers] + all_values[1:]

    # Absolute sheet-column indices, used only to read EXISTING rows out of
    # the full-width sheet. Freshly fetched Metabase rows already contain
    # ONLY the data_columns fields (e.g. A-D,F-Z with no E) in that order,
    # so they're indexed 0..len(data_columns)-1, not by these absolute positions.
    abs_positions = [col_letter_to_idx(l) for l in data_columns] if data_columns else None

    # lecture_id -> (sheet_row_number, existing_values_at_relevant_positions)
    existing_map = {}
    for i in range(header_row, len(all_values)):
        row_vals = all_values[i]
        if id_col >= len(row_vals) or not row_vals[id_col].strip():
            continue
        lec_id = normalise_id(row_vals[id_col])
        row_number = i + 1
        if abs_positions:
            existing_slice = [row_vals[p] if p < len(row_vals) else "" for p in abs_positions]
        else:
            existing_slice = row_vals
        existing_map[lec_id] = (row_number, existing_slice)

    to_append, to_update = [], []   # to_append: (display, write); to_update: (row_number, display, write)
    for idx, dr in enumerate(display_rows):
        wr = write_rows[idx]
        lec_id = normalise_id(dr[id_col])
        if lec_id in existing_map:
            row_number, existing_slice = existing_map[lec_id]
            # Truncate/align lengths defensively (e.g. sheet has trailing
            # blank padding beyond what Metabase returns). Compared against
            # `dr` (display text) - what's actually shown in the sheet today,
            # including for a date column, is its DATE/DATE_TIME-formatted
            # display value, which matches format_for_sheets' output exactly
            # once this fix has run once - see date_to_serial.
            cmp_existing = existing_slice[:len(dr)] if not data_columns else existing_slice
            if not values_equal(cmp_existing, dr):
                to_update.append((row_number, dr, wr))
        else:
            to_append.append((dr, wr))
    if to_update:
        log.info("[%s] queued %d row update(s)", tab_name, len(to_update))
    if to_append:
        log.info("[%s] queued %d new row(s)", tab_name, len(to_append))

    # --- build the writes for this tab: value_batch (USER_ENTERED, non-date
    # cells) and date_batch (RAW serial numbers, date cells) each go out as
    # one batched Sheets API call, plus one formatting call for date_batch's
    # ranges - 3 calls total regardless of how many rows/blocks changed, well
    # inside the 60-writes/min quota. ---
    value_batch, date_batch, fmt_requests = [], [], []
    sheet_id = ws.id

    for row_number, dr, wr in to_update:
        if data_columns:
            for letters, block_positions in group_contiguous_columns(data_columns):
                emit_block_writes(letters, block_positions, row_number, row_number,
                                   [dr], [wr], date_col_positions, date_col_has_time,
                                   value_batch, date_batch, fmt_requests, sheet_id)
        else:
            letters = [idx_to_letter(i) for i in range(len(dr))]
            emit_block_writes(letters, list(range(len(dr))), row_number, row_number,
                               [dr], [wr], date_col_positions, date_col_has_time,
                               value_batch, date_batch, fmt_requests, sheet_id)

    if to_append:
        start_row = len(all_values) + 1
        end_row = start_row + len(to_append) - 1
        append_display = [dr for dr, wr in to_append]
        append_write = [wr for dr, wr in to_append]
        if data_columns:
            for letters, block_positions in group_contiguous_columns(data_columns):
                emit_block_writes(letters, block_positions, start_row, end_row,
                                   append_display, append_write, date_col_positions, date_col_has_time,
                                   value_batch, date_batch, fmt_requests, sheet_id)
        else:
            letters = [idx_to_letter(i) for i in range(len(append_display[0]))]
            emit_block_writes(letters, list(range(len(append_display[0]))), start_row, end_row,
                               append_display, append_write, date_col_positions, date_col_has_time,
                               value_batch, date_batch, fmt_requests, sheet_id)

    if value_batch:
        with_retry(ws.batch_update, value_batch, value_input_option="USER_ENTERED")
        log.info("[%s] wrote non-date changes (%d range block(s))", tab_name, len(value_batch))
    if date_batch:
        with_retry(ws.batch_update, date_batch, value_input_option="RAW")
        log.info("[%s] wrote date changes as real Sheets dates (%d range block(s))", tab_name, len(date_batch))
    if fmt_requests:
        with_retry(ws.spreadsheet.batch_update, {"requests": fmt_requests})
        log.info("[%s] applied DATE/DATE_TIME number format to %d date range(s)", tab_name, len(fmt_requests))

    if not to_append and not to_update:
        log.info("[%s] up to date, nothing changed", tab_name)

    time.sleep(BATCH_PAUSE_SECONDS)
    return display_rows, len(to_append), len(to_update)


def preflight_check_tabs(sheet, expected_tabs):
    '''Logs actual tab names vs expected, so name mismatches (like a
    trailing space or slightly different wording) are caught before syncing.
    Does not fail the run by itself — sync_tab() will still error clearly on
    any individual missing tab, this is just an early, complete heads-up.'''
    actual = [ws.title for ws in sheet.worksheets()]
    log.info("Tabs found in the Google Sheet: %s", actual)
    missing_tabs = [t for t in expected_tabs if t not in actual]
    if missing_tabs:
        log.warning("⚠️  These expected tabs were NOT found (check exact spelling/spacing): %s",
                    missing_tabs)
    else:
        log.info("All expected tabs found. ✅")
    return missing_tabs


# ═══════════════════════════════════════════════════════════════════════════
# RUN
# ═══════════════════════════════════════════════════════════════════════════
try:
    sheet = safe_open_by_key(GOOGLE_SHEET_ID)

    expected_tabs = list(SHEET_CARD_MAP.keys())
    preflight_check_tabs(sheet, expected_tabs)

    run_stats = {}   # tab_name -> {"new": n, "updated": m}
    card_ids = resolve_card_ids(SHEET_CARD_MAP)

    for tab_name, cfg in SHEET_CARD_MAP.items():
        try:
            rows, new_count, updated_count = sync_tab(
                sheet, tab_name, card_ids[tab_name], cfg["id_col"],
                data_columns=cfg.get("data_columns"),
                timeout=cfg.get("timeout", 180),
            )
            run_stats[tab_name] = {"new": new_count, "updated": updated_count}
        except Exception:
            log.exception("Failed syncing tab '%s' - continuing with the rest", tab_name)
            run_stats[tab_name] = {"new": "ERROR", "updated": "ERROR"}

    # ── SUMMARY ──────────────────────────────────────────────────────
    print(f"\nSync finished at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    name_width = max(len(k) for k in run_stats) + 2
    print(f"{'Tab':<{name_width}} {'New':>6} {'Updated':>9}")
    print("-" * (name_width + 18))
    total_new, total_updated, any_errors = 0, 0, False
    for tab_name, stats in run_stats.items():
        new_v, upd_v = stats["new"], stats["updated"]
        print(f"{tab_name:<{name_width}} {new_v:>6} {upd_v:>9}")
        if isinstance(new_v, int):
            total_new += new_v
        else:
            any_errors = True
        if isinstance(upd_v, int):
            total_updated += upd_v
        else:
            any_errors = True

    print("-" * (name_width + 18))
    print(f"{'Total':<{name_width}} {total_new:>6} {total_updated:>9}")

except Exception as e:
    print(f"❌ Pipeline failed: {e}")
    traceback.print_exc()
    sys.exit(1)

mins, secs = divmod(time.time() - start_time, 60)
print(f"\n🎯 M330 sync completed in {int(mins)}m {int(secs)}s")

if any_errors:
    print("⚠️  One or more tabs failed to sync — see ERROR rows above and the "
          "logged exceptions for details. Exiting non-zero so this run is flagged.")
    sys.exit(1)

sys.exit(0)
