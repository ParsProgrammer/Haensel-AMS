# Haensel AMS Attribution Pipeline

Python solution for the Haensel AMS data engineering challenge. The pipeline reads the provided SQLite database, builds customer journeys, calls the IHC attribution API in chunks, writes attribution results back to SQLite, creates `channel_reporting`, and exports the final CSV with `CPO` and `ROAS`.

## Deliverables

- Python pipeline code in `src/attribution_pipeline/`
- Final exported report: `outputs/channel_reporting.csv`
- This README as the short design report, including design choices, assumptions, validation, and possible improvements

## Setup

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -e ".[dev]"
```

The challenge assets are expected at:

- `challenge_data/challenge.db`
- `challenge_db_create.sql`

## Run The Pipeline

Set the IHC credentials from the IHC dashboard, then run:

```powershell
$env:IHC_API_KEY = "your-api-key"
$env:IHC_CONV_TYPE_ID = "your-conversion-type-id"
haensel-attribution
```

The default output is:

```text
outputs/channel_reporting.csv
```

For development only, the pipeline can run without API credentials using equal-split attribution:

```powershell
haensel-attribution --offline-attribution
```

The pipeline also accepts a conversion time range. `--start-date` is inclusive and `--end-date` is exclusive:

```powershell
haensel-attribution --start-date 2023-09-01 --end-date 2023-09-04
```

## Design

I structured the code as a set of small task functions so the same pipeline could be mapped to an orchestrator such as Airflow later:

- `initialize_schema`: creates the missing output tables from `challenge_db_create.sql`
- `build_customer_journeys`: queries sessions and conversions and builds API-ready customer journey rows
- `compute_attribution`: chunks the journey rows and calls the IHC API
- `write_attribution_results`: writes IHC output to `attribution_customer_journey`
- `refresh_channel_reporting`: rebuilds `channel_reporting` from SQLite tables
- `export_channel_reporting`: exports the final CSV and adds `CPO` and `ROAS`

The database remains the source of truth throughout the process. API results are persisted first, and reporting is derived from the database rather than from in-memory intermediate objects.

## Customer Journey Logic

For each conversion, the pipeline selects all sessions for the same `user_id` that happened strictly before the conversion timestamp:

```sql
datetime(session.event_date || ' ' || session.event_time)
    < datetime(conversion.conv_date || ' ' || conversion.conv_time)
```

The IHC API expects one row in each journey to have `conversion = 1`. Since the source database stores conversions separately from sessions, I mark the latest pre-conversion session in each journey as the conversion row for the API payload. The original conversion itself is still kept in the `conversions` table and used later for revenue aggregation.

## API Handling

The IHC API is called via:

```text
POST https://api.ihc-attribution.com/v1/compute_ihc?conv_type_id=<conversion-type-id>
```

with:

```text
Content-Type: application/json
x-api-key: <api-token>
```

The request body is:

```json
{
  "customer_journeys": [
    {
      "conversion_id": "...",
      "session_id": "...",
      "timestamp": "2023-09-01 13:01:28",
      "channel_label": "...",
      "holder_engagement": 1,
      "closer_engagement": 0,
      "conversion": 0,
      "impression_interaction": 0
    }
  ]
}
```

The public IHC documentation mentions limits of 100 customer journeys and 3000 sessions per request, but the free test account returned a stricter 200-session limit. The implementation therefore chunks on both:

- maximum 100 customer journeys per request
- maximum 200 sessions per request

Transient HTTP responses such as `429`, `500`, `502`, `503`, and `504` are retried with exponential backoff.

## Reporting Logic

`attribution_customer_journey` stores one row per attributed conversion/session pair:

```text
conv_id, session_id, ihc
```

`channel_reporting` is rebuilt from the database by date and channel:

- `cost`: total marketing cost for all sessions on that date/channel
- `ihc`: sum of IHC attribution for attributed sessions on that date/channel
- `ihc_revenue`: sum of `ihc * revenue`
- `CPO`: `cost / ihc`, null when `ihc = 0`
- `ROAS`: `ihc_revenue / cost`, null when `cost = 0`

I interpret `cost` as total spend by date/channel, not only the cost of sessions that received attribution. This better matches a reporting table where spend should reflect all marketing activity for the channel/date.

## Assumptions

- All source timestamps are UTC and can be safely compared as combined date/time strings in SQLite.
- Only sessions strictly before the conversion timestamp are included in a journey.
- Conversions with no prior sessions are excluded because there is no session-level touchpoint to receive attribution.
- Missing session costs are treated as `0.0`.
- The latest pre-conversion session is marked as `conversion = 1` for the IHC API because conversions are stored separately from sessions in the source schema.
- API credentials are provided through environment variables and are not stored in the repository.
- The offline equal-split mode is only for development and testing, not for final attribution output.

## Validation

The final real IHC run completed with:

```text
Customer journeys: 1940
Journey sessions: 3703
Attribution rows: 3703
Report rows: 150
```

The pipeline validates that the sum of `ihc` is `1.0` for each `conv_id` after writing attribution results. The project also includes focused pytest coverage for:

- building customer journeys only from sessions before conversion
- chunking requests by journey/session limits
- calculating `channel_reporting` cost, IHC, and IHC revenue correctly

Run tests with:

```powershell
pytest
```

## What Could Be Improved

- Persist raw API request/response payloads for auditability and easier reruns.
- Add structured logging instead of console output.
- Add incremental refresh logic so only affected conversion dates/channels are recalculated.
- Add CI to run tests automatically on each commit.
- Add stronger data-quality checks for duplicate sessions, unusual timestamps, and conversions with no journey.
- Package Docker or Airflow DAG examples if this needed to run as a scheduled production workflow.
