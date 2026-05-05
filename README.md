# Project Members
## RA2512051010008 - THARUN KUMAR M D
## RA2512052010031 - NANDHAKUMAR K P
## RA2512052010032 - MUKESH P R
## RA2512052010064 - SRIVATHSAN K
## RA2512052010017 - RAJESH S
## RA2512052010044 - Aluwala Vivek Reddy


# 🛒 SpadeX — E-Commerce Big Data Solution

> A full-stack cloud data engineering project that solves four critical industrial problems faced by modern e-commerce organizations — built with PySpark, Kafka, Hadoop, and real-time streaming on a cloud-native architecture.

---

## 📌 Table of Contents

- [Project Overview](#-project-overview)
- [Industrial Problems Solved](#-industrial-problems-solved)
- [Architecture](#-architecture)
- [Tech Stack](#-tech-stack)
- [Project Structure](#-project-structure)
- [Pipeline Walkthrough](#-pipeline-walkthrough)
- [Dashboard & KPIs](#-dashboard--kpis)
- [Getting Started](#-getting-started)
- [Results & Outputs](#-results--outputs)
- [Future Enhancements](#-future-enhancements)

---

## 🧠 Project Overview

**SpadeX** is a cloud-based big data pipeline designed for an e-commerce organization struggling with slow reporting, siloed customer data, team inefficiencies, and security vulnerabilities. This project demonstrates how a modern data stack — combining batch and stream processing, graph identity resolution, and zero-trust governance — can transform an organization's data capabilities end to end.

The solution is implemented in a **Google Colab / cloud notebook environment** using PySpark with HDFS simulation, Kafka-style streaming, YARN resource management, and a full analytics dashboard.

---

## 🚨 Industrial Problems Solved

| # | Problem | Solution |
|---|---------|----------|
| 01 | **Slow Reporting** | Lambda + Kappa Architecture using Spark batch + structured streaming |
| 02 | **No Customer 360** | Graph Identity Resolution to unify customer profiles across touchpoints |
| 03 | **Data Team Struggles** | Modern Data Stack (MDS) — Airbyte, dbt, Spark, MLflow, Feast |
| 04 | **Security Concerns** | Zero-Trust Governance — RBAC, AWS KMS, Great Expectations, Audit API |

---

## 🏗️ Architecture

The full pipeline is organized across five horizontal layers, each solving a distinct concern:

```
┌─────────────────────────────────────────────────────────────┐
│                      SpadeX Data Platform                   │
├──────────┬──────────┬───────────┬──────────┬───────────────┤
│  INGEST  │  STORE   │  PROCESS  │  GOVERN  │     SERVE     │
├──────────┼──────────┼───────────┼──────────┼───────────────┤
│  Kafka   │  S3 Lake │   Flink   │   RBAC   │   360 API     │
│ Debezium │Snowflake │  Spark    │  AWS KMS │   Looker      │
│  Airbyte │  Redis   │   dbt     │  Atlas   │  ML Infer     │
│  Kinesis │  Neo4j   │   Feast   │Gr.Expect │  Audit API    │
│          │          │  MLflow   │          │               │
└──────────┴──────────┴───────────┴──────────┴───────────────┘
```

> Every layer solves one problem — and they reinforce each other.

---

## 🛠️ Tech Stack

### Core Processing
![PySpark](https://img.shields.io/badge/PySpark-3.5.1-E25A1C?style=flat&logo=apachespark&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=flat&logo=python&logoColor=white)
![Kafka](https://img.shields.io/badge/Kafka-Streaming-231F20?style=flat&logo=apachekafka&logoColor=white)

### Storage & Warehouse
![S3](https://img.shields.io/badge/AWS_S3-Lake-FF9900?style=flat&logo=amazons3&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-Warehouse-29B5E8?style=flat&logo=snowflake&logoColor=white)
![Redis](https://img.shields.io/badge/Redis-Cache-DC382D?style=flat&logo=redis&logoColor=white)
![Neo4j](https://img.shields.io/badge/Neo4j-Graph_DB-008CC1?style=flat&logo=neo4j&logoColor=white)

### Orchestration & Ingestion
![Airbyte](https://img.shields.io/badge/Airbyte-Ingestion-615EFF?style=flat)
![Debezium](https://img.shields.io/badge/Debezium-CDC-FF4D4D?style=flat)
![Kinesis](https://img.shields.io/badge/AWS_Kinesis-Streaming-FF9900?style=flat&logo=amazonaws&logoColor=white)

### ML & Feature Store
![MLflow](https://img.shields.io/badge/MLflow-Tracking-0194E2?style=flat&logo=mlflow&logoColor=white)
![Feast](https://img.shields.io/badge/Feast-Feature_Store-4B0082?style=flat)

### Governance & Security
![AWS KMS](https://img.shields.io/badge/AWS_KMS-Encryption-FF9900?style=flat&logo=amazonaws&logoColor=white)
![Great Expectations](https://img.shields.io/badge/Great_Expectations-DQ-FF6600?style=flat)

### Visualization
![Matplotlib](https://img.shields.io/badge/Matplotlib-Charts-11557C?style=flat)
![Seaborn](https://img.shields.io/badge/Seaborn-Plots-4C72B0?style=flat)
![Looker](https://img.shields.io/badge/Looker-BI-4285F4?style=flat&logo=looker&logoColor=white)

---

## 📁 Project Structure

```
spadex/
├── hdfs_simulation/
│   ├── raw/
│   │   ├── customers.csv        # 1,000 synthetic customer records
│   │   ├── orders.csv           # 5,000 synthetic order records
│   │   └── clickstream.csv      # 8,000 clickstream events
│   ├── landing/                 # HDFS landing zone simulation
│   ├── processed/
│   │   └── customer360/         # Parquet output — Customer 360 View
│   ├── reports/
│   │   └── monthly_revenue/     # CSV — Monthly revenue by category
│   └── streaming_input/         # Batch CSVs for streaming simulation
├── warehouse/                   # Spark SQL warehouse directory
└── code_file_cloud.ipynb        # Main pipeline notebook
```

---

## 🔄 Pipeline Walkthrough

### Step 1 — Environment Setup
Installs all dependencies: `pyspark`, `faker`, `kafka-python`, `pandas`, `matplotlib`, `seaborn`, `plotly`. Initializes base directories simulating an HDFS-like file system.

### Step 2 — Data Generation
Generates three realistic synthetic datasets using `Faker`:
- **Customers** — 1,000 records (ID, name, city, age, registration date)
- **Orders** — 5,000 records (order ID, customer ID, date, amount, category)
- **Clickstream** — 8,000 events (session ID, customer ID, event type, timestamp, page)

### Step 3 — HDFS Simulation
Simulates an HDFS `PUT` operation by moving raw files to a dedicated landing zone, mimicking a real Hadoop ingestion workflow.

### Step 4 — Spark Session Initialization
Launches a PySpark session (`SpadeX_BigData_Pipeline`) with 4GB executor memory, connected to the simulated warehouse.

### Step 5 — Batch Processing Pipeline
Runs the core Spark pipeline:
- **Customer 360 View** — Aggregates total spend, total orders, and last purchase date per customer, joined with customer metadata. Output written as Parquet.
- **Monthly Revenue Report** — Groups orders by month and category, computing revenue and order count. Output written as partitioned CSV.

### Step 6 — Spark Structured Streaming
Simulates real-time clickstream ingestion:
- Generates **8 streaming batches** (50 events each) with 0.5s intervals
- Reads as a Spark Structured Stream
- Applies **5-minute windowed aggregation** by event type
- Outputs live to console (complete mode)

### Step 7 — YARN Resource Management Simulation
Simulates job submission and completion via a mock `YARNResourceManager` class, demonstrating cluster resource orchestration concepts.

### Step 8 — Analytics Dashboard
Generates a 4-panel matplotlib/seaborn dashboard:
1. **Total Revenue by Category** — Bar chart
2. **Daily Revenue Trend** — Time series line chart
3. **Customer Age Distribution** — Histogram with KDE
4. **Clickstream Event Funnel** — Horizontal bar chart (view → click → add_to_cart → purchase)

---

## 📊 Dashboard & KPIs

After running the pipeline, the following KPIs are computed and printed:

| Metric | Description |
|--------|-------------|
| **Total Revenue** | Sum of all order amounts |
| **Total Orders** | Count of all processed orders |
| **Unique Customers** | Distinct customers who placed orders |
| **Avg Order Value** | Mean transaction amount |
| **Top Category** | Category with highest total revenue |

---

## 🚀 Getting Started

### Prerequisites

- Python 3.8+
- Java 8 or 11 (required for PySpark)
- Google Colab (recommended) or a local Spark environment

### Run on Google Colab (Recommended)

1. Clone or upload `code_file_cloud.ipynb` to Google Colab.
2. Set runtime type to **High RAM** for best performance.
3. Run all cells sequentially — each step prints a ✅ confirmation.

```bash
# Local setup (optional)
pip install pyspark==3.5.1 faker kafka-python pandas matplotlib seaborn plotly
jupyter notebook code_file_cloud.ipynb
```

### Execution Order

```
1. Install packages
2. Setup environment
3. Generate data
4. Simulate HDFS PUT
5. Start Spark session
6. Run batch pipeline
7. Run streaming simulation
8. Generate dashboard
9. Simulate YARN job management
```

---

## ✅ Results & Outputs

| Output | Format | Location |
|--------|--------|----------|
| Customer 360 View | Parquet | `hdfs_simulation/processed/customer360/` |
| Monthly Revenue Report | CSV | `hdfs_simulation/reports/monthly_revenue/` |
| Analytics Dashboard | PNG (inline) | Notebook output |
| KPI Summary | Console print | Notebook output |
| Streaming Aggregations | Console print | Notebook output |

---

## 🔮 Future Enhancements

- [ ] Integrate **real Apache Kafka** for production streaming
- [ ] Deploy on **AWS EMR** or **Databricks** for full-scale execution
- [ ] Add **Neo4j graph identity resolution** for true Customer 360
- [ ] Implement **Great Expectations** data quality checks
- [ ] Build **Looker** or **Grafana** dashboard integration
- [ ] Add **dbt models** for transformation layer
- [ ] Enable **MLflow** experiment tracking for churn prediction models
- [ ] Apply **AWS KMS + RBAC** policies for zero-trust governance

---

## 📄 License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.

---

## 🙋‍♂️ Author

Built as part of an end-to-end e-commerce data engineering capstone.  
Feel free to fork, star ⭐, and raise issues!

---

> *"Every layer solves one problem — and they reinforce each other."*
