# Real-Time Network Traffic Anomaly Detection

This project demonstrates an end-to-end real-time anomaly detection pipeline using PySpark Structured Streaming, Apache Kafka, and Streamlit. The system continuously ingests web server logs, aggregates traffic using sliding windows with watermark mechanisms, and flags abnormal bursts (e.g., DDoS attacks) on a live dashboard.

## 🏗️ Architecture & Data Flow

1. **Data Ingestion (`producer.py`)**: 
   Reads preprocessed log data (JSON format) and continuously streams it into the Kafka topic `web-logs`. It also provides an interactive mechanism to manually inject massive burst traffic to simulate DDoS attacks.
2. **Stream Processing (`spark_processor.py`)**:
   Powered by PySpark Structured Streaming. It subscribes to the Kafka topic, extracts event timestamps, and performs windowed aggregations (traffic count per time window). To guarantee intra-process concurrency safety, aggregated results are written to a local SQLite database (`metrics.db`) via a custom `foreachBatch` sink.
3. **Live Dashboard (`dashboard.py`)**:
   Built with Streamlit and Plotly. It polls the SQLite database periodically to fetch the latest sliding windows and visualizes the traffic. If the traffic exceeds a specified threshold, the UI triggers a visual red alarm.

## 🛠️ Prerequisites & Setup

Ensure the following dependencies are installed and correctly added to your system environment variables (especially for Windows):
- **Python 3.8 - 3.10** (Managed via Conda, environment name: `base` or custom)
- **Java 11 or 17** (`JAVA_HOME` configured)
- **Hadoop Winutils** (`HADOOP_HOME` configured for PySpark on Windows)
- **Apache Kafka & Zookeeper** (Running locally on `localhost:9092`)
- Required Python Packages: `pyspark`, `kafka-python`, `streamlit`, `plotly`, `pandas`

## 🚀 Execution & Usage

PowerShell automation scripts are provided to sequentialize the startup of the pipeline.

**Important**: Make sure your local Kafka and Zookeeper servers are up and running before executing the scripts.

### Option 1: Run the Optimized Pipeline (Latest Architecture)
To run the state-of-the-art version which solves late-data-drop and async reading issues:
```powershell
.\test.ps1
```
*(This script spawns 3 separate PowerShell windows, activates the Conda `base` environment, and sequentially starts the Spark Processor, Kafka Producer, and the Headless Streamlit Dashboard on port 8501).*

### Option 2: Run the Baseline Pipeline
To run the initial/legacy version for A/B comparison and testing:
```powershell
.\test_init.ps1
```

## 📊 Viewing the Dashboard & Simulating Attacks

1. Open your browser and navigate to: `http://localhost:8501`
2. You will see a dynamic line chart auto-scaling based on normal traffic.
3. **Simulate an Attack**: Go to the newly opened terminal running `producer.py` and **press the `Enter` key**. 
4. The terminal will log `[ATTACK] Injected 500 burst events`.
5. Observe the Dashboard: The chart will spike dramatically, and the UI will trigger a red alert indicating an anomaly.

## ⚙️ Key Parameters Explained

The reliability of this stream processing architecture heavily relies on several crucial time and threshold parameters:

### PySpark Processor Parameters (`spark_processor.py`)
- `window("timestamp", "10 seconds", "2 seconds")`: 
  **Sliding Window Logic**. Traffic is grouped into 10-second intervals, evaluated and shifted forward every 2 seconds. Thus, a single event contributes to 5 overlapping windows.
- `WATERMARK_DELAY = "30 seconds"`: 
  **Tolerance for Late Data**. Tells PySpark to maintain window states in memory for an extra 30 seconds. This prevents massive burst injections or network delays from being discarded as "late data".
- `TRIGGER_INTERVAL = "5 seconds"`: 
  The micro-batch physics execution rate. Spark evaluates the DAG and updates the SQLite DB every 5 seconds.

### Dashboard & Alert Parameters (`dashboard.py`)
- `THRESHOLD = 300`: 
  The absolute traffic count. If any 10-second window exceeds 300 requests, it is flagged as anomalous.
- `ALERT_LOOKBACK_WINDOWS = 3`: 
  Instead of only querying the singular "latest" window (which might be partially computed due to Spark's async update mode), the dashboard scans the maximum traffic of the **3 most recent windows**. This completely eliminates false-negatives during alert detection.

### Producer Parameters (`producer.py`)
- `ATTACK_COUNT = 500`: 
  The volume of concurrent requests injected upon pressing 'Enter'. All 500 events are assigned the exact same `timestamp` to ensure they are aggregated into the same spark window without being split across boundaries.