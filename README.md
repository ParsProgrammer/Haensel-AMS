# Haensel AMS Attribution Pipeline

This is a Python solution for the Haensel AMS data engineering challenge. It reads the provided SQLite database, builds conversion customer journeys, computes IHC attribution, stores attribution output back into SQLite, builds channel reporting, and exports the final CSV with `CPO` and `ROAS`.

## Setup

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -e ".[dev]"
```

The challenge assets are expected at:

- `challenge_data/challenge.db`
- `challenge_db_create.sql`

## Run With The IHC API

Create a free IHC account, then set the credentials provided by IHC:

```powershell
$env:IHC_API_KEY = "your-api-key"
$env:IHC_CONV_TYPE_ID = "your-conversion-type-id"
haensel-attribution
```

## Run Locally Without API Credentials

For development and validation only, the pipeline can compute equal-split attribution per conversion:

```powershell
haensel-attribution --offline-attribution
```

The output CSV is written to `outputs/channel_reporting.csv`.

## Optional Time Range

The pipeline accepts a conversion time range. `--start-date` is inclusive and `--end-date` is exclusive.

```powershell
haensel-attribution --start-date 2023-09-01 --end-date 2023-09-04
```

## Tests

```powershell
pytest
```

## Pipeline Steps

1. Initialize missing SQLite tables from `challenge_db_create.sql`.
2. Build customer journeys by joining `conversions` to earlier `session_sources` for the same `user_id`.
3. Transform each journey into IHC API records.
4. Chunk API calls by the documented journey limit and the observed free-plan limit of 200 sessions per request.
5. Write `conversion_id`, `session_id`, and `ihc` into `attribution_customer_journey`.
6. Aggregate total channel/date session cost, IHC orders, and IHC revenue into `channel_reporting`.
7. Export `channel_reporting` with calculated `CPO = cost / ihc` and `ROAS = ihc_revenue / cost`.
