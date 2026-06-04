from __future__ import annotations

import argparse
from pathlib import Path

from attribution_pipeline.pipeline import env_config, run_pipeline


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the IHC attribution challenge pipeline.")
    parser.add_argument("--db-path", default="challenge_data/challenge.db")
    parser.add_argument("--schema-path", default="challenge_db_create.sql")
    parser.add_argument("--output-csv", default="outputs/channel_reporting.csv")
    parser.add_argument("--start-date", help="Inclusive conversion timestamp/date lower bound.")
    parser.add_argument("--end-date", help="Exclusive conversion timestamp/date upper bound.")
    parser.add_argument(
        "--offline-attribution",
        action="store_true",
        help="Use equal-split attribution locally instead of calling the IHC API.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = env_config(
        db_path=args.db_path,
        schema_path=args.schema_path,
        output_csv=args.output_csv,
        start_date=args.start_date,
        end_date=args.end_date,
        offline_attribution=args.offline_attribution,
    )
    stats = run_pipeline(config)
    print("Pipeline completed")
    print(f"Customer journeys: {stats['journeys_sent']}")
    print(f"Journey sessions: {stats['sessions_sent']}")
    print(f"Attribution rows: {stats['attribution_rows']}")
    print(f"Report rows: {stats['report_rows']}")
    print(f"CSV: {Path(args.output_csv).resolve()}")


if __name__ == "__main__":
    main()
