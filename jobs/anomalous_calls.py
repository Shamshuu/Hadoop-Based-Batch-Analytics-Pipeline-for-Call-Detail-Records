import argparse
import sys
import hashlib
import math
from collections import defaultdict
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
import json
import os
from datetime import datetime, timezone

def custom_partitioner(key):
    # key is caller_id (string)
    # Return a deterministic integer hash
    return int(hashlib.md5(key.encode('utf-8')).hexdigest(), 16)

def process_partition(iterator):
    # Group records by caller_id
    records_by_caller = defaultdict(list)
    for caller_id, row in iterator:
        records_by_caller[caller_id].append(row)
        
    anomalies = []
    for caller_id, records in records_by_caller.items():
        durations = [r["duration_sec"] for r in records]
        n = len(durations)
        if n == 0:
            continue
        mean = sum(durations) / n
        # Calculate standard deviation (population stddev)
        variance = sum((x - mean) ** 2 for x in durations) / n
        stddev = math.sqrt(variance)
        
        # Identify anomalies
        for r in records:
            dur = r["duration_sec"]
            if stddev > 0:
                if abs(dur - mean) > 3 * stddev:
                    anomalies.append((
                        r["caller_id"],
                        r["timestamp"],
                        int(dur),
                        float(mean),
                        float(stddev)
                    ))
    return anomalies

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
        .appName("anomalous_call_detection") \
        .getOrCreate()

    job_name = "anomalous_call_detection"
    input_path = args.input
    output_path = f"/output/anomalous_call_detection/{args.run_id}"

    try:
        # Read CSV
        df = spark.read.csv(input_path, header=True, inferSchema=True)
        input_count = df.count()

        # Convert to RDD of (caller_id, dict)
        rdd = df.rdd.map(lambda r: (r["caller_id"], r.asDict()))

        # Repartition RDD using the custom partitioner
        num_partitions = 20
        partitioned_rdd = rdd.partitionBy(num_partitions, custom_partitioner)

        # Process each partition to find anomalies
        anomalous_rdd = partitioned_rdd.mapPartitions(process_partition)

        # Convert anomalous RDD back to DataFrame
        schema = StructType([
            StructField("caller_id", StringType(), False),
            StructField("call_timestamp", StringType(), False),
            StructField("duration_sec", IntegerType(), False),
            StructField("user_mean_duration", FloatType(), False),
            StructField("user_stddev", FloatType(), False)
        ])

        anomalous_df = spark.createDataFrame(anomalous_rdd, schema)

        # Write to output (coalesce(1) to make it a single file)
        anomalous_df.coalesce(1).write.mode("overwrite").csv(output_path, header=False)

        # Output record count
        output_count = anomalous_df.count()

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
