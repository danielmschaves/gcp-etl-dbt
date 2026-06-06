import json
import os
from datetime import datetime
from pathlib import Path
from typing import Dict

import duckdb
import fire
from loguru import logger

from ingestion.bigquery import (
    build_ecommerce_query,
    get_bigquery_client,
    get_bigquery_results,
)
from ingestion.duck import (
    connect_to_md,
    create_table_from_pyarrow_tables,
    load_aws_credentials,
    write_to_md_from_duckdb,
    write_to_s3_from_duckdb,
)
from ingestion.models import (
    EcommerceJobParameters,
    TableValidationError,
    get_max_timestamp,
    validate_table,
)

_WATERMARK_FILE = Path("logs/watermarks.json")


def _load_watermarks() -> Dict[str, str]:
    if _WATERMARK_FILE.exists():
        return json.loads(_WATERMARK_FILE.read_text())
    return {}


def _save_watermarks(watermarks: Dict[str, str]) -> None:
    _WATERMARK_FILE.parent.mkdir(parents=True, exist_ok=True)
    _WATERMARK_FILE.write_text(json.dumps(watermarks, indent=2))


def main(params: EcommerceJobParameters) -> None:
    start_time = datetime.now()
    watermarks = _load_watermarks()

    bigquery_client = get_bigquery_client(project_name=params.gcp_project)
    queries = build_ecommerce_query(params, watermarks=watermarks)

    pyarrow_tables = get_bigquery_results(
        queries=queries,
        table_names=params.table_names,
        bigquery_client=bigquery_client,
    )

    # Validate and collect only tables that pass — skipping invalid ones rather
    # than aborting the whole run.
    validated_tables = {}
    updated_watermarks = dict(watermarks)

    for table_name, pa_tbl in pyarrow_tables.items():
        try:
            validate_table(pa_tbl, table_name)
        except TableValidationError as exc:
            logger.error(f"Skipping '{table_name}': {exc}")
            continue

        validated_tables[table_name] = pa_tbl

        # Advance watermark to the max timestamp seen in this batch
        new_ts = get_max_timestamp(pa_tbl)
        if new_ts:
            updated_watermarks[table_name] = new_ts

    if not validated_tables:
        logger.warning("No tables passed validation — nothing to load.")
        return

    with duckdb.connect() as conn:
        create_table_from_pyarrow_tables(conn, validated_tables)

        for table_name in validated_tables:
            logger.info(f"Sinking '{table_name}' to {params.destination}")

            if "local" in params.destination:
                conn.execute(f"COPY {table_name} TO '{table_name}.csv';")

            if "s3" in params.destination:
                load_aws_credentials(conn, params.aws_profile)
                write_to_s3_from_duckdb(conn, [table_name], params.s3_path)

            if "md" in params.destination:
                connect_to_md(conn, os.environ["MOTHERDUCK_TOKEN"])
                write_to_md_from_duckdb(
                    conn,
                    table=table_name,
                    remote_database="ecommerce",
                )

    _save_watermarks(updated_watermarks)

    elapsed = (datetime.now() - start_time).total_seconds()
    logger.info(
        f"Pipeline completed in {elapsed // 60:.0f}m {elapsed % 60:.2f}s"
    )


if __name__ == "__main__":
    fire.Fire(
        lambda **kwargs: main(
            EcommerceJobParameters(
                **{
                    k: v.split(",") if k == "table_names" and isinstance(v, str) else v
                    for k, v in kwargs.items()
                }
            )
        )
    )
