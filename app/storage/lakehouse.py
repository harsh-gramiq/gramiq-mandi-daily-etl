"""Partitioned Columnar Lakehouse Export (Parquet / DuckDB Layer).

Writes Snappy-compressed, dictionary-encoded Parquet files partitioned by year and month.
Enables sub-millisecond analytical queries and RAG vector store ingestion for Krishi Mitra/Khata.
"""

import os
from pathlib import Path
from typing import Any


def export_to_lakehouse(
    records: list[dict[str, Any]],
    target_date_iso: str,
    output_dir: str = "data/lakehouse",
) -> dict[str, Any]:
    """
    Exports clean validated mandi records to partitioned Parquet files.
    Partition structure: {output_dir}/year=YYYY/month=MM/mandi_YYYY-MM-DD.parquet
    """
    if not records:
        return {"status": "EMPTY", "file_path": None, "record_count": 0}

    year = target_date_iso[:4]
    month = target_date_iso[5:7]

    partition_dir = Path(output_dir) / f"year={year}" / f"month={month}"
    partition_dir.mkdir(parents=True, exist_ok=True)

    parquet_file = partition_dir / f"mandi_{target_date_iso}.parquet"
    latest_file = Path(output_dir) / "mandi_latest.parquet"

    # Convert records to standardized dictionary
    data: dict[str, list[Any]] = {
        "trade_date": [],
        "state": [],
        "district": [],
        "market": [],
        "commodity": [],
        "variety": [],
        "grade": [],
        "normalized_modal_price_qtl": [],
        "min_price_qtl": [],
        "max_price_qtl": [],
        "arrival_tonnes": [],
        "source_system": [],
    }

    for r in records:
        data["trade_date"].append(str(r.get("trade_date") or target_date_iso))
        data["state"].append(str(r.get("state") or "Unknown"))
        data["district"].append(str(r.get("district") or "Unknown"))
        data["market"].append(str(r.get("market") or "Unknown"))
        data["commodity"].append(str(r.get("commodity") or "Unknown"))
        data["variety"].append(str(r.get("variety") or "Standard"))
        data["grade"].append(str(r.get("grade") or "FAQ"))
        data["normalized_modal_price_qtl"].append(float(r.get("normalized_modal_price_qtl") or 0.0))
        data["min_price_qtl"].append(float(r.get("min_price_qtl") or r.get("min_price") or 0.0))
        data["max_price_qtl"].append(float(r.get("max_price_qtl") or r.get("max_price") or 0.0))
        data["arrival_tonnes"].append(float(r.get("raw_arrival_quantity") or 0.0))
        data["source_system"].append("AGMARKNET_2.0")

    try:
        import pyarrow as pa
        import pyarrow.parquet as pq

        schema = pa.schema([
            ("trade_date", pa.string()),
            ("state", pa.string()),
            ("district", pa.string()),
            ("market", pa.string()),
            ("commodity", pa.string()),
            ("variety", pa.string()),
            ("grade", pa.string()),
            ("normalized_modal_price_qtl", pa.float64()),
            ("min_price_qtl", pa.float64()),
            ("max_price_qtl", pa.float64()),
            ("arrival_tonnes", pa.float64()),
            ("source_system", pa.string()),
        ])

        table = pa.Table.from_pydict(data, schema=schema)
        pq.write_table(
            table,
            str(parquet_file),
            compression="snappy",
            use_dictionary=True,
        )
        # Also write latest
        pq.write_table(
            table,
            str(latest_file),
            compression="snappy",
            use_dictionary=True,
        )
        file_size = os.path.getsize(parquet_file)
        print(f"  [Lakehouse] ✅ Exported {len(records):,} rows to {parquet_file} ({file_size / 1024:.1f} KB Snappy Parquet)")
        return {
            "status": "SUCCESS",
            "format": "parquet",
            "file_path": str(parquet_file),
            "latest_path": str(latest_file),
            "record_count": len(records),
            "file_size_bytes": file_size,
        }
    except ImportError:
        # Fallback to JSON Lines for testing environments without pyarrow
        json_file = partition_dir / f"mandi_{target_date_iso}.jsonl"
        import json
        with open(json_file, "w", encoding="utf-8") as f:
            for i in range(len(data["trade_date"])):
                row = {k: v[i] for k, v in data.items()}
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
        print(f"  [Lakehouse] ℹ️ PyArrow not installed; exported {len(records):,} rows to {json_file}")
        return {
            "status": "SUCCESS",
            "format": "jsonl",
            "file_path": str(json_file),
            "latest_path": None,
            "record_count": len(records),
            "file_size_bytes": os.path.getsize(json_file),
        }
