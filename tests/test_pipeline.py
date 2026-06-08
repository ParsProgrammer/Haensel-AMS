from pathlib import Path
import sqlite3

import pandas as pd

from attribution_pipeline.pipeline import (
    PipelineConfig,
    build_customer_journeys,
    chunk_customer_journeys,
    compute_equal_attribution,
    initialize_schema,
    refresh_channel_reporting,
)


SCHEMA = """
CREATE TABLE IF NOT EXISTS conversions (
    conv_id text NOT NULL,
    user_id text NOT NULL,
    conv_date text NOT NULL,
    conv_time text NOT NULL,
    revenue real NOT NULL,
    PRIMARY KEY(conv_id)
);

CREATE TABLE IF NOT EXISTS session_costs (
    session_id text NOT NULL,
    cost real,
    PRIMARY KEY(session_id)
);

CREATE TABLE IF NOT EXISTS session_sources (
    session_id text NOT NULL,
    user_id text NOT NULL,
    event_date text NOT NULL,
    event_time text NOT NULL,
    channel_name text NOT NULL,
    holder_engagement INTEGER NOT NULL,
    closer_engagement INTEGER NOT NULL,
    impression_interaction INTEGER NOT NULL,
    PRIMARY KEY(session_id)
);

CREATE TABLE IF NOT EXISTS attribution_customer_journey (
    conv_id text NOT NULL,
    session_id text NOT NULL,
    ihc real NOT NULL,
    PRIMARY KEY(conv_id,session_id)
);

CREATE TABLE IF NOT EXISTS channel_reporting (
    channel_name text NOT NULL,
    date text NOT NULL,
    cost real NOT NULL,
    ihc real NOT NULL,
    ihc_revenue real NOT NULL,
    PRIMARY KEY(channel_name,date)
);
"""


def make_config(tmp_path: Path) -> PipelineConfig:
    db_path = tmp_path / "challenge.db"
    schema_path = tmp_path / "schema.sql"
    schema_path.write_text(SCHEMA, encoding="utf-8")
    config = PipelineConfig(
        db_path=db_path,
        schema_path=schema_path,
        output_csv=tmp_path / "channel_reporting.csv",
        offline_attribution=True,
    )
    initialize_schema(config)
    return config


def seed_rows(config: PipelineConfig) -> None:
    with sqlite3.connect(config.db_path) as connection:
        connection.executemany(
            "INSERT INTO session_sources VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                ("s1", "u1", "2023-09-01", "10:00:00", "Email", 1, 0, 0),
                ("s2", "u1", "2023-09-01", "11:00:00", "Search", 0, 1, 0),
                ("s3", "u1", "2023-09-01", "13:00:00", "Email", 1, 1, 0),
                ("s4", "u2", "2023-09-01", "10:00:00", "Email", 1, 1, 0),
            ],
        )
        connection.executemany(
            "INSERT INTO conversions VALUES (?, ?, ?, ?, ?)",
            [
                ("c1", "u1", "2023-09-01", "12:00:00", 100.0),
                ("c2", "u2", "2023-09-01", "12:00:00", 50.0),
            ],
        )
        connection.executemany(
            "INSERT INTO session_costs VALUES (?, ?)",
            [("s1", 10.0), ("s2", 20.0), ("s3", 30.0), ("s4", 40.0)],
        )


def test_build_customer_journeys_uses_only_sessions_before_conversion(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    seed_rows(config)

    journeys = build_customer_journeys(config)

    assert set(journeys["session_id"]) == {"s1", "s2", "s4"}
    c1 = journeys[journeys["conversion_id"] == "c1"]
    assert c1["conversion"].tolist() == [0, 1]


def test_chunk_customer_journeys_respects_journey_and_session_limits() -> None:
    journeys = pd.DataFrame(
        [
            {"conversion_id": "c1", "session_id": "s1"},
            {"conversion_id": "c1", "session_id": "s2"},
            {"conversion_id": "c2", "session_id": "s3"},
            {"conversion_id": "c3", "session_id": "s4"},
        ]
    )

    chunks = list(chunk_customer_journeys(journeys, max_journeys=2, max_sessions=3))

    assert [chunk["conversion_id"].nunique() for chunk in chunks] == [2, 1]
    assert [len(chunk) for chunk in chunks] == [3, 1]


def test_refresh_channel_reporting_keeps_total_session_cost(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    seed_rows(config)

    journeys = build_customer_journeys(config)
    attribution = compute_equal_attribution(journeys)
    attribution.rename(columns={"conversion_id": "conv_id"}).to_sql(
        "attribution_customer_journey",
        sqlite3.connect(config.db_path),
        if_exists="append",
        index=False,
    )

    refresh_channel_reporting(config)

    with sqlite3.connect(config.db_path) as connection:
        rows = {
            (row[0], row[1]): row[2:]
            for row in connection.execute(
                "SELECT channel_name, date, cost, ihc, ihc_revenue FROM channel_reporting"
            )
        }

    assert rows[("Email", "2023-09-01")][0] == 80.0
    assert rows[("Email", "2023-09-01")][1:] == (1.5, 100.0)
    assert rows[("Search", "2023-09-01")] == (20.0, 0.5, 50.0)
