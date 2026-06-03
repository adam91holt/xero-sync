# xero-sync

A small Node.js service that syncs accounting data from the **Xero API** into **MongoDB**. It runs on a cron schedule, supports full and incremental syncs, and handles Xero's rate limits with exponential backoff.

## What it does

- Pulls Xero entities (invoices, credit notes, payments, contacts, accounts, items, bank transactions, and more) and writes them to a MongoDB database.
- Tracks per-entity sync state (`sync_state.json`) so incremental runs only fetch records modified since the last sync.
- Rate-limit aware: configurable delay, retries, exponential backoff, and jitter.
- Runs continuously on a schedule, or once-and-exit for cron jobs (`RUN_ONCE=true`).

OAuth is handled by a separate process; this service loads the Xero token directly from a token file (`TOKEN_PATH`).

## Requirements

- Node.js 18+
- A MongoDB instance (local or Atlas)
- A valid Xero OAuth token file produced by your auth flow

## Setup

```bash
npm install
cp .env.example .env   # then fill in your values
```

## Configuration

All configuration is via environment variables (see `.env.example`):

| Variable | Description | Default |
| --- | --- | --- |
| `MONGODB_URI` | MongoDB connection string | `mongodb://localhost:27017/` |
| `MONGODB_DB_NAME` | Target database name | `xero_data` |
| `SYNC_SCHEDULE` | Cron expression for sync runs | `0 */4 * * *` |
| `TOKEN_PATH` | Path to the Xero OAuth token JSON | — |
| `SYNC_STATE_PATH` | Path to the sync-state file | `./sync_state.json` |
| `FULL_SYNC` | `true` ignores sync state and does a full sync | `false` |
| `HISTORY_YEARS` | Years of history to pull on a full sync | `10` |
| `DEFAULT_DELAY_MS` | Delay between API calls (ms) | `1000` |
| `MAX_RETRIES` | Max retries for a failed request | `10` |
| `MAX_BACKOFF_MS` | Max backoff delay (ms) | `300000` |
| `BACKOFF_MULTIPLIER` | Exponential backoff multiplier | `2` |
| `JITTER_MAX_MS` | Max random jitter added to delays (ms) | `1000` |
| `DEFAULT_PAGE_SIZE` | Page size for paginated calls | `100` |
| `RUN_ONCE` | Run a single sync then exit | unset |
| `XERO_SCOPES` | Override the default Xero scopes | comprehensive set |

## Usage

```bash
npm start      # run the service (scheduled syncs)
npm run dev    # run with nodemon for development
```

## Notes

- This repo intentionally contains only the service code. Secrets (`.env`, MCP config, token files) are not committed.
