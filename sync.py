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
    '''Formats a datetime the way Google Sheets reliably auto-recognizes as a
    real Date/Datetime when written with value_input_option=USER_ENTERED -
    no "T", no "Z", no offset (those are what break Sheets' auto-detection).'''
    if dt.hour == 0 and dt.minute == 0 and dt.second == 0 and dt.microsecond == 0:
        return dt.strftime("%Y-%m-%d")
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def normalize_dates_in_row(row):
    '''Rewrites any date/timestamp-looking cell in a row to Sheets-friendly
    text. Non-date cells (IDs, names, numbers) pass through untouched.'''
    out = []
    for v in row:
        dt = try_parse_date(v) if isinstance(v, str) else None
        out.append(format_for_sheets(dt) if dt is not None else v)
    return out


def fetch_card_rows(card_id, timeout=180):
    # Some questions (e.g. large joins/funnels) genuinely take a while to run.
    # metabase_request also retries on timeout, so total worst case is timeout*3.
    resp = metabase_request("POST", f"/api/card/{card_id}/query", timeout=timeout)
    payload = resp.json()
    data = payload.get("data", {})
    rows = data.get("rows", [])
    cols = [c.get("display_name") or c.get("name") for c in data.get("cols", [])]
    log.info("Card %s: fetched %d rows, columns=%s", card_id, len(rows), cols)
    return [normalize_dates_in_row(r) for r in rows]


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


def sync_tab(sheet, tab_name, card_id, id_col, data_columns=None, header_row=1, timeout=180):
    '''Fetches fresh Metabase rows and reconciles them against the sheet:
    - a lecture ID not already in the sheet -> appended as a new row
    - a lecture ID already present, with any value different (even a small
      numeric change) -> that row is updated in place
    - a lecture ID present with identical values -> left untouched
    Returns (fresh_rows, new_count, updated_count).'''
    ws = sheet.worksheet(tab_name)
    fresh_rows = fetch_card_rows(card_id, timeout=timeout)

    all_values = with_retry(ws.get_all_values)
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

    to_append, to_update = [], []
    for r in fresh_rows:
        lec_id = normalise_id(r[id_col])
        fresh_slice = list(r)
        if lec_id in existing_map:
            row_number, existing_slice = existing_map[lec_id]
            # Truncate/align lengths defensively (e.g. sheet has trailing
            # blank padding beyond what Metabase returns).
            cmp_existing = existing_slice[:len(fresh_slice)] if not data_columns else existing_slice
            if not values_equal(cmp_existing, fresh_slice):
                to_update.append((row_number, r))
        else:
            to_append.append(r)

    # --- build ALL writes for this tab as one batch (single Sheets API call,
    # regardless of how many rows/blocks - this is what avoids hitting the
    # 60-writes/min quota when several rows change in one run) ---
    batch_data = []

    for row_number, r in to_update:
        if data_columns:
            for letters, block_positions in group_contiguous_columns(data_columns):
                block_values = [[r[p] for p in block_positions]]
                range_name = f"{letters[0]}{row_number}:{letters[-1]}{row_number}"
                batch_data.append({"range": range_name, "values": block_values})
        else:
            end_letter = idx_to_letter(len(r) - 1)
            range_name = f"A{row_number}:{end_letter}{row_number}"
            batch_data.append({"range": range_name, "values": [r]})
    if to_update:
        log.info("[%s] queued %d row update(s)", tab_name, len(to_update))

    if to_append:
        start_row = len(all_values) + 1
        end_row = start_row + len(to_append) - 1
        if data_columns:
            for letters, block_positions in group_contiguous_columns(data_columns):
                block_values = [[row[p] for p in block_positions] for row in to_append]
                range_name = f"{letters[0]}{start_row}:{letters[-1]}{end_row}"
                batch_data.append({"range": range_name, "values": block_values})
        else:
            end_letter = idx_to_letter(len(to_append[0]) - 1)
            range_name = f"A{start_row}:{end_letter}{end_row}"
            batch_data.append({"range": range_name, "values": to_append})
        log.info("[%s] queued %d new row(s)", tab_name, len(to_append))

    if batch_data:
        with_retry(ws.batch_update, batch_data, value_input_option="USER_ENTERED")
        log.info("[%s] wrote all changes in a single batch call", tab_name)

    if not to_append and not to_update:
        log.info("[%s] up to date, nothing changed", tab_name)

    time.sleep(BATCH_PAUSE_SECONDS)
    return fresh_rows, len(to_append), len(to_update)


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
