from __future__ import annotations

import json
import os
import sqlite3
import time
import http.client
import urllib.error
import urllib.parse
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd


API_URL = "https://api.ihc-attribution.com/v1/compute_ihc"
API_HOST = "api.ihc-attribution.com"
API_PATH = "/v1/compute_ihc"


@dataclass(frozen=True)
class PipelineConfig:
    db_path: Path
    schema_path: Path
    output_csv: Path
    api_key: str | None = None
    conv_type_id: str | None = None
    start_date: str | None = None
    end_date: str | None = None
    offline_attribution: bool = False
    max_journeys_per_request: int = 100
    max_sessions_per_request: int = 200
    request_sleep_seconds: float = 0.0


def connect(db_path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(db_path)
    connection.execute("PRAGMA foreign_keys = ON")
    return connection


def initialize_schema(config: PipelineConfig) -> None:
    schema = config.schema_path.read_text(encoding="utf-8")
    schema = schema[schema.index("CREATE TABLE") :]
    with connect(config.db_path) as connection:
        connection.executescript(schema)


def build_customer_journeys(config: PipelineConfig) -> pd.DataFrame:
    where_clauses: list[str] = []
    params: dict[str, str] = {}

    if config.start_date:
        where_clauses.append("datetime(c.conv_date || ' ' || c.conv_time) >= datetime(:start_date)")
        params["start_date"] = config.start_date
    if config.end_date:
        where_clauses.append("datetime(c.conv_date || ' ' || c.conv_time) < datetime(:end_date)")
        params["end_date"] = config.end_date

    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    query = f"""
        SELECT
            c.conv_id AS conversion_id,
            s.session_id,
            s.event_date || ' ' || s.event_time AS timestamp,
            s.channel_name AS channel_label,
            s.holder_engagement,
            s.closer_engagement,
            s.impression_interaction
        FROM conversions c
        JOIN session_sources s
            ON s.user_id = c.user_id
           AND datetime(s.event_date || ' ' || s.event_time)
               < datetime(c.conv_date || ' ' || c.conv_time)
        {where_sql}
        ORDER BY c.conv_id, datetime(timestamp), s.session_id
    """

    with connect(config.db_path) as connection:
        journeys = pd.read_sql_query(query, connection, params=params)

    if journeys.empty:
        return journeys

    int_columns = ["holder_engagement", "closer_engagement", "impression_interaction"]
    journeys[int_columns] = journeys[int_columns].astype(int)
    journeys["conversion"] = 0

    last_session_index = journeys.groupby("conversion_id")["timestamp"].idxmax()
    journeys.loc[last_session_index, "conversion"] = 1

    return journeys[
        [
            "conversion_id",
            "session_id",
            "timestamp",
            "channel_label",
            "holder_engagement",
            "closer_engagement",
            "conversion",
            "impression_interaction",
        ]
    ]


def chunk_customer_journeys(
    journeys: pd.DataFrame,
    max_journeys: int,
    max_sessions: int,
) -> Iterable[pd.DataFrame]:
    current_frames: list[pd.DataFrame] = []
    current_journey_count = 0
    current_session_count = 0

    for _, journey in journeys.groupby("conversion_id", sort=False):
        session_count = len(journey)
        if session_count > max_sessions:
            raise ValueError(
                f"Journey {journey['conversion_id'].iloc[0]} has {session_count} sessions, "
                f"which exceeds the API session limit of {max_sessions}."
            )

        would_exceed = (
            current_frames
            and (
                current_journey_count + 1 > max_journeys
                or current_session_count + session_count > max_sessions
            )
        )
        if would_exceed:
            yield pd.concat(current_frames, ignore_index=True)
            current_frames = []
            current_journey_count = 0
            current_session_count = 0

        current_frames.append(journey)
        current_journey_count += 1
        current_session_count += session_count

    if current_frames:
        yield pd.concat(current_frames, ignore_index=True)


def compute_attribution(config: PipelineConfig, journeys: pd.DataFrame) -> pd.DataFrame:
    if journeys.empty:
        return pd.DataFrame(columns=["conversion_id", "session_id", "ihc"])

    if config.offline_attribution:
        return compute_equal_attribution(journeys)

    if not config.api_key or not config.conv_type_id:
        raise ValueError(
            "IHC_API_KEY and IHC_CONV_TYPE_ID are required unless --offline-attribution is used."
        )

    results: list[dict] = []
    for chunk in chunk_customer_journeys(
        journeys,
        max_journeys=config.max_journeys_per_request,
        max_sessions=config.max_sessions_per_request,
    ):
        payload = {"customer_journeys": chunk.to_dict(orient="records")}
        results.extend(call_ihc_api(config.api_key, config.conv_type_id, payload))
        if config.request_sleep_seconds:
            time.sleep(config.request_sleep_seconds)

    return pd.DataFrame(results)[["conversion_id", "session_id", "ihc"]]


def call_ihc_api(api_key: str, conv_type_id: str, payload: dict) -> list[dict]:
    path = f"{API_PATH}?conv_type_id={urllib.parse.quote(conv_type_id)}"
    connection = http.client.HTTPSConnection(API_HOST, timeout=60)
    try:
        connection.request(
            "POST",
            path,
            body=json.dumps(payload),
            headers={
                "Content-Type": "application/json",
                "x-api-key": api_key,
            },
        )
        response = connection.getresponse()
        detail = response.read().decode("utf-8", errors="replace")
    finally:
        connection.close()

    if response.status >= 400:
        raise RuntimeError(f"IHC API request failed with HTTP {response.status}: {detail}")

    body = json.loads(detail)

    status_code = body.get("statusCode")
    if status_code not in (200, 206):
        raise RuntimeError(f"IHC API returned statusCode={status_code}: {body}")
    if body.get("partialFailureErrors"):
        raise RuntimeError(f"IHC API returned partial failures: {body['partialFailureErrors']}")

    return body.get("value", [])


def compute_equal_attribution(journeys: pd.DataFrame) -> pd.DataFrame:
    offline = journeys[["conversion_id", "session_id"]].copy()
    offline["ihc"] = 1.0 / offline.groupby("conversion_id")["session_id"].transform("count")
    return offline


def write_attribution_results(config: PipelineConfig, attribution: pd.DataFrame) -> None:
    if attribution.empty:
        return

    output = attribution.rename(columns={"conversion_id": "conv_id"})[
        ["conv_id", "session_id", "ihc"]
    ].copy()
    conversion_ids = output["conv_id"].drop_duplicates().tolist()

    with connect(config.db_path) as connection:
        placeholders = ",".join("?" for _ in conversion_ids)
        connection.execute(
            f"DELETE FROM attribution_customer_journey WHERE conv_id IN ({placeholders})",
            conversion_ids,
        )
        output.to_sql("attribution_customer_journey", connection, if_exists="append", index=False)


def refresh_channel_reporting(config: PipelineConfig) -> None:
    query = """
        INSERT INTO channel_reporting (channel_name, date, cost, ihc, ihc_revenue)
        SELECT
            ss.channel_name,
            ss.event_date AS date,
            SUM(COALESCE(sc.cost, 0.0)) AS cost,
            SUM(acj.ihc) AS ihc,
            SUM(acj.ihc * c.revenue) AS ihc_revenue
        FROM attribution_customer_journey acj
        JOIN session_sources ss
            ON ss.session_id = acj.session_id
        LEFT JOIN session_costs sc
            ON sc.session_id = acj.session_id
        JOIN conversions c
            ON c.conv_id = acj.conv_id
        GROUP BY ss.channel_name, ss.event_date
    """

    with connect(config.db_path) as connection:
        connection.execute("DELETE FROM channel_reporting")
        connection.execute(query)


def export_channel_reporting(config: PipelineConfig) -> pd.DataFrame:
    query = """
        SELECT
            channel_name,
            date,
            cost,
            ihc,
            ihc_revenue,
            CASE WHEN ihc = 0 THEN NULL ELSE cost / ihc END AS CPO,
            CASE WHEN cost = 0 THEN NULL ELSE ihc_revenue / cost END AS ROAS
        FROM channel_reporting
        ORDER BY date, channel_name
    """
    with connect(config.db_path) as connection:
        report = pd.read_sql_query(query, connection)

    config.output_csv.parent.mkdir(parents=True, exist_ok=True)
    report.to_csv(config.output_csv, index=False)
    return report


def validate_attribution(config: PipelineConfig) -> pd.DataFrame:
    query = """
        SELECT conv_id, ROUND(SUM(ihc), 8) AS ihc_sum, COUNT(*) AS sessions
        FROM attribution_customer_journey
        GROUP BY conv_id
        HAVING ABS(SUM(ihc) - 1.0) > 0.000001
    """
    with connect(config.db_path) as connection:
        return pd.read_sql_query(query, connection)


def run_pipeline(config: PipelineConfig) -> dict[str, int]:
    initialize_schema(config)
    journeys = build_customer_journeys(config)
    attribution = compute_attribution(config, journeys)
    write_attribution_results(config, attribution)
    refresh_channel_reporting(config)
    report = export_channel_reporting(config)
    validation_errors = validate_attribution(config)

    if not validation_errors.empty:
        raise ValueError(f"Attribution sums are invalid:\n{validation_errors}")

    return {
        "sessions_sent": len(journeys),
        "journeys_sent": journeys["conversion_id"].nunique() if not journeys.empty else 0,
        "attribution_rows": len(attribution),
        "report_rows": len(report),
    }


def env_config(
    db_path: str,
    schema_path: str,
    output_csv: str,
    start_date: str | None = None,
    end_date: str | None = None,
    offline_attribution: bool = False,
) -> PipelineConfig:
    api_key = os.getenv("IHC_API_KEY")
    conv_type_id = os.getenv("IHC_CONV_TYPE_ID")
    return PipelineConfig(
        db_path=Path(db_path),
        schema_path=Path(schema_path),
        output_csv=Path(output_csv),
        api_key=api_key.strip() if api_key else None,
        conv_type_id=conv_type_id.strip() if conv_type_id else None,
        start_date=start_date,
        end_date=end_date,
        offline_attribution=offline_attribution,
    )
