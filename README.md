# M330 — Metabase → Google Sheets Sync (GitHub Actions Cron)

Scheduled version of `M330_metabase_sync_4.ipynb`. Syncs 9 sheet tabs from
their mapped Metabase questions, reconciling row-by-row (append new lecture
IDs, update changed rows, leave unchanged rows untouched) instead of
overwriting the whole sheet each run.

## Files

```
.
├── .github/workflows/m330-metabase-sync.yml   # scheduled workflow
├── sync.py                                     # the sync script
├── requirements.txt
└── README.md
```

## What changed vs. the notebook

Only the auth layer — all sync logic (retry/backoff, diff-based reconcile,
batched writes, per-tab error isolation, run summary) is untouched:

| Notebook (manual, Colab)                          | This version (unattended, cron)                     |
|-----------------------------------------------------|-------------------------------------------------------|
| `google.colab.auth.authenticate_user()` (your login) | Google service account (`SERVICE_ACCOUNT_JSON`)       |
| `userdata.get('METABASE_API_KEY')` etc.             | Environment variables / GitHub Actions secrets        |

## One-time setup

### 1. Create a Google service account

1. In [Google Cloud Console](https://console.cloud.google.com/), create (or
   reuse) a project → **IAM & Admin → Service Accounts → Create Service
   Account**.
2. Enable the **Google Sheets API** and **Google Drive API** for that
   project (APIs & Services → Library → search each → Enable).
3. On the service account: **Keys → Add Key → Create new key → JSON**. This
   downloads a `.json` file — its full contents go into a GitHub secret
   below.
4. Open the target Google Sheet (`17QTxOXlfY3FLBsaumZhDn0WlNo2xSH1cXXnOf5d-Qk4`
   by default) and **share it** with the service account's email (the
   `client_email` field in the JSON file) as **Editor**.

### 2. Get a Metabase API key

Metabase: profile icon → **Settings → Admin settings → API Keys → Create
API Key**. Copy the value immediately — it's shown only once.

### 3. Add GitHub repo secrets

**Settings → Secrets and variables → Actions → New repository secret**:

| Secret name            | Value                                                        |
|--------------------------|--------------------------------------------------------------|
| `METABASE_API_KEY`       | The API key from step 2                                     |
| `SERVICE_ACCOUNT_JSON`   | The **entire contents** of the JSON file from step 1        |

`METABASE_URL`, `GOOGLE_SHEET_ID`, and `METABASE_DASHBOARD_ID` all have
working defaults baked into `sync.py` and don't need secrets unless you want
to override them — if you do, add them as secrets too and uncomment the
matching lines in the workflow file's `env:` block.

### 4. Push these files to the repo

Commit `sync.py`, `requirements.txt`, and
`.github/workflows/m330-metabase-sync.yml` to your default branch — GitHub
only picks up workflows living at `.github/workflows/*.yml` on a pushed
branch.

## Running it

- **On schedule**: every 4 hours by default. Edit the `cron:` line in the
  workflow file to change this (always UTC, format: `minute hour
  day-of-month month day-of-week`).
- **Manually**: **Actions** tab → **M330 Metabase Sync** → **Run workflow**.

## How the sync itself behaves (unchanged from the notebook)

- Each tab is reconciled against its mapped Metabase question by **lecture
  ID** (column A unless configured otherwise): new IDs are appended, changed
  rows are updated in place, unchanged rows are left alone.
- All writes for a tab go out as a **single batched API call**, regardless of
  how many rows changed — this is what keeps the job well under Sheets'
  60-writes/minute quota even when a lot changes at once.
- The `"RFD - Class wise - Viewer breakdown to RFD"` tab has a live formula
  in column E, so its sync deliberately writes only columns A-D and F-Z,
  skipping E so the formula is never overwritten.
- If one tab fails (bad card ID, Metabase error, etc.), the run logs it and
  **keeps going** with the remaining tabs — you get partial results instead
  of losing the whole run. The job still exits non-zero at the end if
  anything failed, so GitHub flags the run red for follow-up, but you can
  see exactly which tabs succeeded in the summary at the bottom of the log.

## If a run fails

Check **Actions** tab → the failed run → **Run sync** step. Look for:

- **`401 Unauthorized`** from Metabase → `METABASE_API_KEY` is wrong,
  revoked, or its group can't see one of the mapped questions.
- **`SpreadsheetNotFound`** → the sheet ID is wrong, or it hasn't been
  shared with the service account (the error message names the exact email
  to add).
- **A specific tab's `ERROR` in the summary table** → check the logged
  exception right above it for that tab — usually either the Metabase
  question itself errored (open it directly in Metabase to confirm) or the
  tab name in the sheet doesn't exactly match `SHEET_CARD_MAP`.
