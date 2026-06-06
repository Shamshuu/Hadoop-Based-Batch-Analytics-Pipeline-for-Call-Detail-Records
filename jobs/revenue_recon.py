import argparse
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum
import json
import os
from datetime import datetime, timezone

def write_manifest(job_name, run_id, input_path, output_path, input_count, output_count, status="SUCCESS"):
    manifest = {
        "job_name": job_name,
        "run_id": run_id,
        "execution_timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "input_path": input_path,
        "output_path": output_path,
        "input_record_count": int(input_count),
        "output_record_count": int(output_count),
        "status": status
    }
    os.makedirs(output_path, exist_ok=True)
    manifest_file = os.path.join(output_path, "_MANIFEST.json")
    with open(manifest_file, "w") as f:
        json.dump(manifest, f, indent=2)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--input", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName("revenue_reconciliation") \
        .getOrCreate()

    job_name = "revenue_reconciliation"
    input_path = args.input
    output_path = f"/output/revenue_reconciliation/{args.run_id}"

    try:
        # Read CSV
        df = spark.read.csv(input_path, header=True, inferSchema=True)
        input_count = df.count()

        # Sum of all charge_amount
        revenue_df = df.agg(spark_sum("charge_amount").alias("total_revenue"))

        # Write to output (coalesce(1) to make it a single file)
        revenue_df.coalesce(1).write.mode("overwrite").csv(output_path, header=False)

        # Output record count
        output_count = revenue_df.count()

        # Write success manifest
        write_manifest(job_name, args.run_id, input_path, output_path, input_count, output_count, "SUCCESS")

    except Exception as e:
        print(f"Error executing job: {e}", file=sys.stderr)
        write_manifest(job_name, args.run_id, input_path, output_path, 0, 0, "FAILURE")
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
