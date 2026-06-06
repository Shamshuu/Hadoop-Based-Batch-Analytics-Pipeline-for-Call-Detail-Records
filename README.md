# Hadoop-Based Batch Analytics Pipeline for Call Detail Records (CDRs)

A containerized, scalable big data analytics pipeline designed to simulate, orchestrate, and process millions of Call Detail Records (CDRs). Telecom providers generate massive volumes of connection logs daily. This pipeline processes these datasets at scale using a distributed architecture to solve critical business problems including fraud detection, financial audit reconciliation, network congestion profiling, and high-value customer identification.

---

## 🏗️ System Architecture

The pipeline packages a multi-container big-data environment into a unified Docker Compose application:

```mermaid
graph TD
    A[data-generator] -->|1. Generates 2.05M CDRs CSV| B[Shared Volume /data]
    C[Airflow Scheduler / Webserver] -->|2. Orchestrates & Triggers DAGs| D[Spark Master]
    D -->|3. Distributes Tasks| E[Spark Worker]
    
    B -->|4. Reads Raw Data| D
    E -->|5. Performs Distributed Processing| E
    E -->|6. Saves Aggregated CSVs & Lineage Manifests| F[Shared Volume /output]

    subgraph Hadoop Distributed System (Simulated via HDFS)
        N[namenode] <---> DN[datanode]
    end
```

### Infrastructure Components

*   **Apache Spark (PySpark):** Serves as the high-performance distributed execution engine. The master ([spark-master](file:///c:/GPP/Week21/Hadoop-Based-Batch-Analytics-Pipeline-for-Call-Detail-Records/docker-compose.yml#L31-L53)) and worker ([spark-worker](file:///c:/GPP/Week21/Hadoop-Based-Batch-Analytics-Pipeline-for-Call-Detail-Records/docker-compose.yml#L54-L69)) nodes run queries in-memory across distributed datasets.
*   **Apache Airflow:** Acts as the pipeline orchestrator. The Airflow container runs a sequential executor, manages job scheduling, schedules automatic retries, and exposes both a web UI (on port `8080`) and a REST API to trigger analytical runs.
*   **Hadoop (HDFS namenode & datanode):** Runs as the containerized distributed storage layer, mirroring a production-ready enterprise cluster.
*   **Docker & Docker Compose:** Coordinates the services, volumes, networks, and environment setup without requiring manual system-level installations.

---

## 📂 Project Directory Structure

*   [docker-compose.yml](file:///c:/GPP/Week21/Hadoop-Based-Batch-Analytics-Pipeline-for-Call-Detail-Records/docker-compose.yml) — Configuration defining the multi-service containerized cluster.
*   [Dockerfile](file:///c:/GPP/Week21/Hadoop-Based-Batch-Analytics-Pipeline-for-Call-Detail-Records/Dockerfile) — Custom Airflow image definition including JDK 17, Apache Spark bin, and PySpark bindings.
*   [run_pipeline.sh](file:///c:/GPP/Week21/Hadoop-Based-Batch-Analytics-Pipeline-for-Call-Detail-Records/run_pipeline.sh) — Bash controller script for triggering jobs and polling execution state.
*   [CurlCommandsToRun.txt](file:///c:/GPP/Week21/Hadoop-Based-Batch-Analytics-Pipeline-for-Call-Detail-Records/CurlCommandsToRun.txt) — Reference sheet for manual HTTP REST triggers.
*   [data/](file:///c:/GPP/Week21/Hadoop-Based-Batch-Analytics-Pipeline-for-Call-Detail-Records/data/)
    *   [generate_records.py](file:///c:/GPP/Week21/Hadoop-Based-Batch-Analytics-Pipeline-for-Call-Detail-Records/data/generate_records.py) — Python script simulating exactly 2,050,000 CDR logs.
    *   [generate_records.sh](file:///c:/GPP/Week21/Hadoop-Based-Batch-Analytics-Pipeline-for-Call-Detail-Records/data/generate_records.sh) — Shell wrapper executing the generator inside containers.
*   [dags/](file:///c:/GPP/Week21/Hadoop-Based-Batch-Analytics-Pipeline-for-Call-Detail-Records/dags/)
    *   [top_callers_dag.py](file:///c:/GPP/Week21/Hadoop-Based-Batch-Analytics-Pipeline-for-Call-Detail-Records/dags/top_callers_dag.py) — Orchestrator file for the High Spender metric.
    *   [tower_heatmap_dag.py](file:///c:/GPP/Week21/Hadoop-Based-Batch-Analytics-Pipeline-for-Call-Detail-Records/dags/tower_heatmap_dag.py) — Orchestrator file for Tower heatmaps.
    *   [anomalous_calls_dag.py](file:///c:/GPP/Week21/Hadoop-Based-Batch-Analytics-Pipeline-for-Call-Detail-Records/dags/anomalous_calls_dag.py) — Orchestrator file for Fraud/Anomaly detection.
    *   [revenue_recon_dag.py](file:///c:/GPP/Week21/Hadoop-Based-Batch-Analytics-Pipeline-for-Call-Detail-Records/dags/revenue_recon_dag.py) — Orchestrator file for financial reconciliation.
*   [jobs/](file:///c:/GPP/Week21/Hadoop-Based-Batch-Analytics-Pipeline-for-Call-Detail-Records/jobs/)
    *   [top_callers.py](file:///c:/GPP/Week21/Hadoop-Based-Batch-Analytics-Pipeline-for-Call-Detail-Records/jobs/top_callers.py) — PySpark aggregation script for spend calculation.
    *   [tower_heatmap.py](file:///c:/GPP/Week21/Hadoop-Based-Batch-Analytics-Pipeline-for-Call-Detail-Records/jobs/tower_heatmap.py) — PySpark datetime mapping and aggregation script.
    *   [anomalous_calls.py](file:///c:/GPP/Week21/Hadoop-Based-Batch-Analytics-Pipeline-for-Call-Detail-Records/jobs/anomalous_calls.py) — PySpark RDD-based statistical outlier detector.
    *   [revenue_recon.py](file:///c:/GPP/Week21/Hadoop-Based-Batch-Analytics-Pipeline-for-Call-Detail-Records/jobs/revenue_recon.py) — PySpark sum reconciliation aggregator.

---

## 📈 Pipeline Features & Business Queries

### 1. Data Generation & Ingestion
*   **Implementation:** [generate_records.py](file:///c:/GPP/Week21/Hadoop-Based-Batch-Analytics-Pipeline-for-Call-Detail-Records/data/generate_records.py)
*   **Logic:** Generates exactly 2,050,000 records. It designs caller-specific normal distributions for call durations and generates:
    *   A simulated "whale caller" (`whale_caller_999`) representing over 10% of total network volume.
    *   A statistical duration outlier rate of 0.1% for anomaly simulation.
    *   Standard telecom attributes: `caller_id`, `receiver_id`, `duration_sec`, `tower_id`, `timestamp`, `call_type` (VOICE/SMS/DATA), and `charge_amount`.

### 2. Financial Auditing: Top Callers by Spend
*   **Airflow DAG:** `top_callers_by_spend_dag` ([top_callers_dag.py](file:///c:/GPP/Week21/Hadoop-Based-Batch-Analytics-Pipeline-for-Call-Detail-Records/dags/top_callers_dag.py))
*   **PySpark Job:** [top_callers.py](file:///c:/GPP/Week21/Hadoop-Based-Batch-Analytics-Pipeline-for-Call-Detail-Records/jobs/top_callers.py)
*   **Logic:** Reads CDRs, groups records by `caller_id`, sums `charge_amount`, sorts spend in descending order, filters the top 100 callers, and writes the output as a coalesced single CSV.

### 3. Network Planning: Tower Heatmap
*   **Airflow DAG:** `tower_utilization_heatmap_dag` ([tower_heatmap_dag.py](file:///c:/GPP/Week21/Hadoop-Based-Batch-Analytics-Pipeline-for-Call-Detail-Records/dags/tower_heatmap_dag.py))
*   **PySpark Job:** [tower_heatmap.py](file:///c:/GPP/Week21/Hadoop-Based-Batch-Analytics-Pipeline-for-Call-Detail-Records/jobs/tower_heatmap.py)
*   **Logic:** Extracts the hour of the day (0–23) from the ISO-8601 timestamps, groups by `tower_id` and `hour_of_day`, and calculates traffic/call counts.

### 4. Fraud Detection: Anomalous Call Duration
*   **Airflow DAG:** `anomalous_call_detection_dag` ([anomalous_calls_dag.py](file:///c:/GPP/Week21/Hadoop-Based-Batch-Analytics-Pipeline-for-Call-Detail-Records/dags/anomalous_calls_dag.py))
*   **PySpark Job:** [anomalous_calls.py](file:///c:/GPP/Week21/Hadoop-Based-Batch-Analytics-Pipeline-for-Call-Detail-Records/jobs/anomalous_calls.py)
*   **Logic:** Implements a distributed statistical anomaly detector:
    1.  Converts the dataframe into a PySpark RDD.
    2.  Uses an MD5-based deterministic custom partitioner (`custom_partitioner`) to guarantee all calls belonging to the same `caller_id` route to the same Spark execution partition.
    3.  Runs a `mapPartitions` function to compute the localized mean ($\mu$) and standard deviation ($\sigma$) of call durations for each user.
    4.  Flags and outputs records where call duration $x$ deviates from the user's average by more than three standard deviations ($|x - \mu| > 3\sigma$).

### 5. Financial Reconciliation: Revenue Verification
*   **Airflow DAG:** `revenue_reconciliation_dag` ([revenue_recon_dag.py](file:///c:/GPP/Week21/Hadoop-Based-Batch-Analytics-Pipeline-for-Call-Detail-Records/dags/revenue_recon_dag.py))
*   **PySpark Job:** [revenue_recon.py](file:///c:/GPP/Week21/Hadoop-Based-Batch-Analytics-Pipeline-for-Call-Detail-Records/jobs/revenue_recon.py)
*   **Logic:** Runs a global sum aggregation over the entire dataset's `charge_amount` column, helping operators verify billing integrity across runs.

### 6. Lineage Tracking: Metadata Manifests
Every Spark job concludes by generating a detailed JSON metadata file named `_MANIFEST.json` inside its run directory. This manifest tracks:
*   `job_name` & `run_id`
*   `execution_timestamp_utc`
*   Input & output file paths
*   Input record count vs. output record count
*   Execution status (`SUCCESS` / `FAILURE`)

---

## 🚀 Getting Started

### 📋 Prerequisites
Ensure you have Docker and Docker Compose installed:
*   [Docker Desktop for Windows](https://www.docker.com/products/docker-desktop/)

### 1. Cluster Initialization
Spin up all cluster nodes (Hadoop, Spark, Airflow, and the Data Generator):
```bash
docker compose up -d
```
The containers will spin up sequentially. The `data-generator` will initialize and automatically run [generate_records.py](file:///c:/GPP/Week21/Hadoop-Based-Batch-Analytics-Pipeline-for-Call-Detail-Records/data/generate_records.py) to write the synthetic CDR dataset onto the shared volume at `data/cdr_data.csv`.

---

## 🛠️ Executing Pipelines

You can trigger and monitor pipeline DAGs in two ways: using the automated controller script or using REST API endpoints via `curl`.

### Method A: Automated Controller Script
Run [run_pipeline.sh](file:///c:/GPP/Week21/Hadoop-Based-Batch-Analytics-Pipeline-for-Call-Detail-Records/run_pipeline.sh) from Git Bash. The script triggers the DAG via Docker CLI, tracks its execution state, and polls until completion.

```bash
# Usage: ./run_pipeline.sh <logical_query_name>
# Supported names: top_callers, tower_heatmap, anomalous_calls, revenue_recon

./run_pipeline.sh top_callers
./run_pipeline.sh tower_heatmap
./run_pipeline.sh anomalous_calls
./run_pipeline.sh revenue_recon
```

### Method B: Airflow REST API (via `curl`)
You can trigger runs remotely by sending POST requests authenticated with `admin:admin`.

#### 1. Trigger Top Callers DAG
```bash
curl -X POST -u admin:admin -H "Content-Type: application/json" \
  -d '{"conf": {"run_id": "top_callers_run"}}' \
  http://localhost:8080/api/v1/dags/top_callers_by_spend_dag/dagRuns
```

#### 2. Trigger Tower Utilization Heatmap DAG
```bash
curl -X POST -u admin:admin -H "Content-Type: application/json" \
  -d '{"conf": {"run_id": "tower_heatmap_run"}}' \
  http://localhost:8080/api/v1/dags/tower_utilization_heatmap_dag/dagRuns
```

#### 3. Trigger Anomalous Call Detection DAG
```bash
curl -X POST -u admin:admin -H "Content-Type: application/json" \
  -d '{"conf": {"run_id": "anomalous_calls_run"}}' \
  http://localhost:8080/api/v1/dags/anomalous_call_detection_dag/dagRuns
```

#### 4. Trigger Revenue Reconciliation DAG
```bash
curl -X POST -u admin:admin -H "Content-Type: application/json" \
  -d '{"conf": {"run_id": "revenue_recon_run"}}' \
  http://localhost:8080/api/v1/dags/revenue_reconciliation_dag/dagRuns
```

#### 5. Checking Job Status
Query the execution status of a DAG run using GET:
```bash
curl -X GET -u admin:admin http://localhost:8080/api/v1/dags/top_callers_by_spend_dag/dagRuns
```

---

## 📊 Verifying Outputs & Lineage

Output CSVs and execution manifests are written to the local `/output` directory, mapped from the container's `/output` volume:

```text
output/
├── anomalous_call_detection/
│   └── <run_id>/
│       ├── _MANIFEST.json
│       └── part-00000-...csv
├── revenue_reconciliation/
│   └── <run_id>/
│       ├── _MANIFEST.json
│       └── part-00000-...csv
├── top_callers_by_spend/
│   └── <run_id>/
│       ├── _MANIFEST.json
│       └── part-00000-...csv
└── tower_utilization_heatmap/
    └── <run_id>/
        ├── _MANIFEST.json
        └── part-00000-...csv
```

### Example Commands to Inspect Results

Verify and view the top lines of output datasets and print their run manifests:

```bash
# View Top Spend Callers
cat output/top_callers_by_spend/top_callers_run/part-*.csv | head -n 10
cat output/top_callers_by_spend/top_callers_run/_MANIFEST.json

# View Tower Utilization Heatmap
cat output/tower_utilization_heatmap/tower_heatmap_run/part-*.csv | head -n 10
cat output/tower_utilization_heatmap/tower_heatmap_run/_MANIFEST.json

# View Anomalous Calls Audit Log
cat output/anomalous_call_detection/anomalous_calls_run/part-*.csv | head -n 10
cat output/anomalous_call_detection/anomalous_calls_run/_MANIFEST.json

# View Global Revenue Reconciliation Sum
cat output/revenue_reconciliation/revenue_recon_run/part-*.csv | head -n 10
cat output/revenue_reconciliation/revenue_recon_run/_MANIFEST.json
```