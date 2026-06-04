# Attribution Pipeline Report

## Design

The solution is organized around separable task functions in `src/attribution_pipeline/pipeline.py` so the same logic could be moved into Airflow tasks later:

- `initialize_schema`
- `build_customer_journeys`
- `compute_attribution`
- `write_attribution_results`
- `refresh_channel_reporting`
- `export_channel_reporting`

The CLI in `src/attribution_pipeline/cli.py` wires those tasks together and exposes optional time-range arguments. The database remains the system of record: API results are written to `attribution_customer_journey`, then the final reporting table is rebuilt from SQLite and exported to CSV.

## Assumptions

- A customer journey contains sessions for the conversion user that occurred strictly before the conversion timestamp.
- The IHC API requires one record in each journey to carry `conversion = 1`. Because the source data stores conversions separately from sessions, the latest session before the conversion is marked as the conversion session in the API payload.
- Conversions with no previous sessions are excluded from attribution because there is no session-level touchpoint to receive credit.
- Missing session costs are treated as `0.0`.
- `CPO` and `ROAS` are exported as null when their denominators are zero.
- The local `--offline-attribution` mode is only for development and reproducible testing without API credentials. It assigns equal credit across sessions in a conversion journey and should be replaced by a real IHC API run for final submission.

## API Handling

The IHC API documentation describes a POST endpoint at `https://api.ihc-attribution.com/v1/compute_ihc` with `conv_type_id` as a URL parameter and an API key in the `x-api-key` header. The API response contains a `value` list with `conversion_id`, `session_id`, and `ihc`. The public documentation mentions 100 customer journeys and 3000 sessions per request, but the free test account returned a stricter 200-session limit. The pipeline therefore chunks requests on both journey count and a conservative 200-session request size.

## What Could Be Improved

- Add retry/backoff around transient API errors and persist raw API responses for auditability.
- Add richer validation, for example comparing conversion coverage and reporting which conversions have no prior sessions.
- Incrementally refresh `channel_reporting` for only the affected dates instead of rebuilding the full table.
- Add unit tests around journey construction, chunking, and reporting aggregation.
- Package the pipeline as an installable project with CI and typed linting.
