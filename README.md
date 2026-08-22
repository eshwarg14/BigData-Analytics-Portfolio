<div align="center">

# 🐘 BigData Analytics Portfolio

### Real-World Big Data Engineering & Analytics Projects

[![Hadoop](https://img.shields.io/badge/Apache_Hadoop-3.4.1-66CCFF?style=for-the-badge&logo=apache&logoColor=white)](https://hadoop.apache.org/)
[![Hive](https://img.shields.io/badge/Apache_Hive-4.0.1-FDEE21?style=for-the-badge&logo=apache&logoColor=black)](https://hive.apache.org/)
[![Pig](https://img.shields.io/badge/Apache_Pig-0.17-F9A800?style=for-the-badge&logo=apache&logoColor=black)](https://pig.apache.org/)
[![Python](https://img.shields.io/badge/Python-3.12-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://python.org/)
[![Java](https://img.shields.io/badge/Java-17-ED8B00?style=for-the-badge&logo=openjdk&logoColor=white)](https://openjdk.org/)
[![License](https://img.shields.io/badge/License-MIT-green?style=for-the-badge)](LICENSE)

*A portfolio of distributed computing and Big Data analytics projects built on the Apache Hadoop ecosystem.*

</div>

---

## 👨‍💻 About This Portfolio

This repository contains **7 industry-style Big Data engineering projects** built on the **Apache Hadoop ecosystem**. Each project solves a real-world analytics problem using distributed computing techniques - from processing e-commerce product reviews at scale to querying movie datasets through a SQL-on-Hadoop warehouse.

The projects progress from foundational MapReduce programming to advanced patterns including custom Partitioners, Distributed Cache joins, Python Hadoop Streaming, Apache Pig dataflows, and Apache Hive warehousing with Partitioning and Bucketing.

> **Author:** Eshwar G

---

## 🗂️ Projects Overview

| # | Project | Technology | Pattern | Domain |
|---|---|---|---|---|
| 1 | [Damaged Product Review Analysis](#1--damaged-product-review-analysis) | Hadoop MapReduce | WordCount | E-Commerce |
| 2 | [Product Sentiment Partition Analysis](#2--product-sentiment-partition-analysis) | Hadoop MapReduce | Custom Partitioner | E-Commerce |
| 3 | [Patient Appointment Data Integration](#3--patient-appointment-data-integration) | Hadoop MapReduce | Reduce-Side Join | Healthcare |
| 4 | [Student Performance Distributed Cache Analysis](#4--student-performance-distributed-cache-analysis) | Hadoop MapReduce | Map-Side Join / Dist. Cache | Education |
| 5 | [Web Server Log Analytics](#5--web-server-log-analytics) | Python + Hadoop Streaming | Streaming MapReduce | DevOps / Security |
| 6 | [E-Commerce Sales Analysis](#6--e-commerce-sales-analysis) | Apache Pig | Dataflow / Pig Latin | E-Commerce |
| 7 | [Movies & Directors Analytics](#7--movies--directors-analytics) | Apache Hive | Partitioning + Bucketing + HiveQL | Entertainment |

---

## 🧭 Hadoop Ecosystem Overview

### 🐘 Apache Hadoop

Apache Hadoop is an open-source framework for **distributed storage and processing** of large datasets across clusters of commodity hardware. It is the foundation of the modern Big Data ecosystem.

**Core Components:**

| Component | Role |
|---|---|
| **HDFS** | Hadoop Distributed File System - stores data across multiple nodes with replication for fault tolerance |
| **YARN** | Yet Another Resource Negotiator - manages cluster resources and job scheduling |
| **MapReduce** | Distributed computation model: Map (transform) → Shuffle (sort+group) → Reduce (aggregate) |
| **Common** | Utilities and libraries shared by all Hadoop modules |

**Key Architecture Concepts:**

- **NameNode** - Master node managing HDFS metadata and file namespace.
- **DataNode** - Worker nodes storing actual data blocks (default block size: 128 MB).
- **ResourceManager** - YARN master allocating CPU/memory across jobs.
- **NodeManager** - Per-node YARN agent managing container lifecycle.
- **SecondaryNameNode** - Periodically checkpoints the NameNode's edit log (not a backup NameNode).

---

### 🔄 MapReduce

MapReduce is the native computation engine of Hadoop - a two-phase distributed processing model:

```
INPUT DATA (HDFS)
      │
      ▼  TextInputFormat splits input into chunks
┌─────────────────────────────────────────────────┐
│  MAP PHASE                                      │
│  • Each mapper processes one input split        │
│  • Applies user-defined map() function          │
│  • Emits intermediate (Key, Value) pairs        │
└──────────────────────┬──────────────────────────┘
                       │
                       ▼  Shuffle & Sort
              (sorted by key, grouped)
                       │
┌──────────────────────▼──────────────────────────┐
│  REDUCE PHASE                                   │
│  • Each reducer receives all values for a key   │
│  • Applies user-defined reduce() function       │
│  • Emits final (Key, Value) output              │
└──────────────────────┬──────────────────────────┘
                       │
                  OUTPUT (HDFS)
               part-r-00000, part-r-00001, ...
```

**Advanced MapReduce Patterns covered in this portfolio:**

| Pattern | Project | Description |
|---|---|---|
| Basic WordCount | Project 1 | Token frequency count |
| Custom Partitioner | Project 2 | Route keys to specific reducers |
| Reduce-Side Join | Project 3 | Join two datasets at reducer |
| Map-Side Join | Project 4 | Join via Distributed Cache at mapper |
| Combiner | - | Local reducer to minimise shuffle traffic |

---

### 🌊 Hadoop Streaming

Hadoop Streaming allows **any language** (Python, Ruby, Shell) to write MapReduce jobs by communicating through **stdin/stdout** pipes. Hadoop Streaming wraps these scripts and manages YARN execution.

```bash
hadoop jar hadoop-streaming.jar \
  -input  /hdfs/input \
  -output /hdfs/output \
  -mapper  "python3 mapper.py" \
  -reducer "python3 reducer.py"
```

**Benefits:**
- 🐍 No Java required - use Python, Bash, Perl, R, etc.
- ⚡ Faster prototyping and iteration cycles.
- 🔧 Easy integration with existing data science scripts.
- 📚 Familiar languages for analysts and data scientists.

---

### 🐷 Apache Pig

Apache Pig is a **dataflow scripting platform** for processing large datasets on Hadoop. It uses **Pig Latin** - a high-level, procedural language that compiles to MapReduce.

**Why Pig over raw MapReduce?**
- 10× fewer lines of code for the same transformation.
- Built-in operators: `FILTER`, `GROUP`, `JOIN`, `FOREACH`, `ORDER`, `DISTINCT`.
- Automatically optimises the MR job plan (Job DAG).
- Suitable for ETL pipelines and exploratory data analysis.

**Core Pig Latin Operators used in this portfolio:**

| Operator | Purpose |
|---|---|
| `LOAD` | Read data from HDFS with schema |
| `FILTER` | Row-level filtering (WHERE equivalent) |
| `FOREACH GENERATE` | Column projection and computed fields |
| `GROUP BY` | Aggregate rows by key |
| `ORDER BY` | Global sort (triggers a MR sort job) |
| `DUMP` | Print relation to console |
| `STORE` | Write output to HDFS |

---

### 🐝 Apache Hive

Apache Hive is a **SQL-on-Hadoop data warehouse** that translates HiveQL (SQL-like) queries into MapReduce or Tez jobs. It enables analysts who know SQL to query petabyte-scale data stored in HDFS.

**Hive Architecture:**

```
User (HiveQL)
     │
     ▼
HiveServer2 / Beeline (JDBC)
     │
     ▼
Query Compiler + Optimizer
     │
     ▼
Execution Engine (MapReduce / Tez)
     │
     ▼
HDFS (ORC / Parquet / TextFile)
```

**Advanced Hive features covered:**

| Feature | Description | Benefit |
|---|---|---|
| **Partitioning** | Splits table into genre=X subdirectories | Partition pruning - skip irrelevant data |
| **Bucketing** | Hash-splits data into N files per partition | Efficient JOIN + TABLESAMPLE |
| **ORC Format** | Columnar, compressed binary format | 70–90% smaller than CSV, fast aggregations |
| **Dynamic Partitioning** | Auto-route rows to correct partition | Simplifies bulk data loading |
| **Beeline** | Production JDBC CLI for HiveServer2 | Multi-user concurrent access |

---

### 🗃️ Distributed Cache

Hadoop's **Distributed Cache** mechanism broadcasts a small file (e.g., a reference/lookup table) from HDFS to the **local filesystem of every mapper node** before job execution. This enables highly efficient **Map-Side Joins**.

```java
// Register file in Distributed Cache (Driver)
job.addCacheFile(new URI("hdfs:///path/to/small_table.csv#alias.csv"));

// Access in Mapper's setup() - already on local disk
File cachedFile = new File("./alias.csv");
BufferedReader reader = new BufferedReader(new FileReader(cachedFile));
// Load into HashMap for O(1) lookups
```

**When to use:**
- Reference/dimension table is small (fits in mapper memory).
- Join is needed in the Map phase to avoid expensive shuffle.
- Configuration files or lookup dictionaries needed on all nodes.

---

## 📁 Project Deep-Dives

### 1. 🛒 Damaged Product Review Analysis

**📂 [`Damaged-Product-Review-Analysis-Hadoop/`](./Damaged-Product-Review-Analysis-Hadoop/)**

Processes 1,000 Flipkart/Amazon electronics reviews to count the word **"damaged"** per product using Hadoop MapReduce WordCount.

```
Mapper  → Tokenise reviews → emit (ProductName, 1) when "damaged" found
Reducer → Sum counts per product
Output  → Product-wise damage complaint frequency
```

**Key Result:** JBL Flip 5 Bluetooth Speaker had 22 damage mentions - highest in the dataset, flagging a packaging quality issue.

---

### 2. 📊 Product Sentiment Partition Analysis

**📂 [`Product-Sentiment-Partition-Analysis/`](./Product-Sentiment-Partition-Analysis/)**

Extends WordCount with a **Custom Partitioner** to split reviews into negative ("damaged") and positive ("good") output files.

```
Mapper      → Emit (ProductName_damaged, 1) or (ProductName_good, 1)
Partitioner → _damaged → Reducer 0 → part-r-00000
              _good    → Reducer 1 → part-r-00001
```

**Key Result:** Separate negative/positive feedback files enable side-by-side product quality comparison.

---

### 3. 🏥 Patient Appointment Data Integration

**📂 [`Patient-Appointment-Data-Integration/`](./Patient-Appointment-Data-Integration/)**

Implements a **Reduce-Side Join** using `MultipleInputs` to merge 500 patient registration records with 500+ appointment records.

```
PatientMapper     → (PatientID, PD~Name,DOB,BloodType)
AppointmentMapper → (PatientID, AD~Date,Doctor,Dept,Status)
Reducer           → Outer Join → Unified Patient+Appointment record
```

**Key Result:** Unified healthcare records enabling doctor consultations, billing, and inactive patient identification.

---

### 4. 🎓 Student Performance Distributed Cache Analysis

**📂 [`Student-Performance-Distributed-Cache-Analysis/`](./Student-Performance-Distributed-Cache-Analysis/)**

Uses **Hadoop Distributed Cache** to broadcast `s.csv` (student info) to all mapper nodes, enabling a zero-shuffle **Map-Side Join** with marks data.

```
setup()   → Load s.csv from Distributed Cache into HashMap
map()     → Lookup student name by regno → emit joined record
reduce()  → Compute Pass (all subjects ≥ 50) or Fail
```

**Key Result:** Efficient result processing without reduce-side join overhead.

---

### 5. 🌐 Web Server Log Analytics

**📂 [`Web-Server-Log-Analytics-Hadoop-Streaming/`](./Web-Server-Log-Analytics-Hadoop-Streaming/)**

Python-based **Hadoop Streaming** pipeline analysing Apache access logs for IP traffic, URL popularity, and HTTP status code distribution.

```
mapper.py  → Parse log line → emit IP, URL, STATUS counts
reducer.py → Aggregate → print 3-section analytics report
```

**Key Result:** 35 successful (200), 4 server errors (500), 4 forbidden (403) - security alert warranted.

---

### 6. 🛍️ E-Commerce Sales Analysis

**📂 [`ECommerce-Sales-Analysis-Apache-Pig/`](./ECommerce-Sales-Analysis-Apache-Pig/)**

Apache Pig Dataflow processing 500 sales records through 3 pipelines in a single script run.

```
Analysis 1 → Best-selling products by quantity
Analysis 2 → Category-wise revenue (price × qty)
Analysis 3 → Peak buying hours (hour-of-day distribution)
```

**Key Results:** Electronics tops revenue (₹1.55 Cr), Dumbbells lead quantity (88 units), 1 PM is peak shopping hour.

---

### 7. 🎬 Movies & Directors Analytics

**📂 [`Movies-Analytics-Apache-Hive/`](./Movies-Analytics-Apache-Hive/)**

Full Apache Hive warehouse demonstration: partitioned + bucketed ORC table, partition pruning, sampling, aggregations, and JOIN across 30 movies in 6 genres.

```
Partitioned BY genre → 6 HDFS subdirectories
Clustered BY movie_id INTO 4 BUCKETS
Queries: Pruning | Sampling | Aggregations | JOIN
```

**Key Results:** KGF Chapter 2 tops Action revenue (₹1,250 Cr); Interstellar dominates Sci-Fi (₹4,800 Cr).

---

## 🛠️ Tools & Environment

| Tool | Version | Purpose |
|---|---|---|
| Ubuntu | 24.04 LTS | Operating system |
| Apache Hadoop | 3.4.1 | Distributed computing framework |
| Apache Pig | 0.17.0 | Dataflow scripting |
| Apache Hive | 4.0.1 | SQL-on-Hadoop warehouse |
| Java | OpenJDK 17 | MapReduce development |
| Python | 3.12 | Hadoop Streaming scripts |
| Eclipse IDE | 2024-06 | Java development + JAR packaging |
| Nano / Kate | - | Script editing on Linux |
| Beeline | 4.0.1 | HiveServer2 CLI client |

---

## 🧰 Skills Demonstrated

### Big Data Engineering
- ✅ Hadoop cluster setup (pseudo-distributed single-node mode)
- ✅ HDFS operations: `mkdir`, `put`, `get`, `ls`, `cat`, `head`
- ✅ YARN job submission and monitoring
- ✅ MapReduce program development, JAR packaging, and execution
- ✅ Hadoop Streaming with Python scripts

### Data Processing Patterns
- ✅ WordCount and keyword frequency analysis
- ✅ Custom Partitioner for parallel sentiment routing
- ✅ Reduce-Side Join with `MultipleInputs`
- ✅ Map-Side Join with Distributed Cache
- ✅ Multi-output MapReduce jobs

### Apache Pig
- ✅ Pig Latin scripting for ETL and aggregation
- ✅ LOAD, FILTER, GROUP, FOREACH, ORDER, STORE, DUMP operators
- ✅ Revenue computation via FOREACH expressions
- ✅ Multiple STORE operations in a single script

### Apache Hive
- ✅ DDL: CREATE TABLE with PARTITIONED BY and CLUSTERED BY
- ✅ DML: LOAD DATA, INSERT INTO PARTITION
- ✅ DQL: SELECT, GROUP BY, ORDER BY, JOIN, LIMIT
- ✅ ORC columnar storage format
- ✅ Partition pruning and TABLESAMPLE bucketing queries

### General Data Engineering
- ✅ CSV dataset preparation and schema design
- ✅ Data quality handling (null filtering, header skipping)
- ✅ Output interpretation and business insight extraction
- ✅ Performance comparison across join strategies

---

## 🚀 Getting Started

### Prerequisites

```bash
# Java 17
sudo apt install openjdk-17-jdk

# Hadoop 3.4.1 - download from https://hadoop.apache.org/releases.html
tar -xzf hadoop-3.4.1.tar.gz
export HADOOP_HOME=/opt/hadoop-3.4.1
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

# Python 3.12
sudo apt install python3.12

# Apache Pig (for Project 6)
tar -xzf pig-0.17.0.tar.gz
export PIG_HOME=/opt/pig-0.17.0
export PATH=$PATH:$PIG_HOME/bin

# Apache Hive (for Project 7)
tar -xzf apache-hive-4.0.1-bin.tar.gz
export HIVE_HOME=/opt/apache-hive-4.0.1-bin
export PATH=$PATH:$HIVE_HOME/bin
```

### Start Hadoop Services

```bash
su - hadoop
start-dfs.sh
start-yarn.sh
jps  # Verify: NameNode, DataNode, ResourceManager, NodeManager, SecondaryNameNode
```

### Run Any Project

```bash
cd <project-directory>
bash run_job.sh
# OR follow the step-by-step README in each project folder
```

---

## 📊 Learning Path

```
Beginner                        Intermediate                    Advanced
    │                               │                               │
    ▼                               ▼                               ▼
Project 1                      Project 3                      Project 6
(MapReduce WordCount)          (Reduce-Side Join)             (Apache Pig)
    │                               │                               │
Project 2                      Project 4                      Project 7
(Custom Partitioner)           (Distributed Cache)            (Apache Hive)
                                    │
                               Project 5
                               (Hadoop Streaming)
```

Start with **Project 1** for MapReduce fundamentals, then progress through join patterns, streaming, and finally high-level abstractions (Pig & Hive).

---

## 🔮 Future Extensions

- ⚡ **Apache Spark** - Migrate batch jobs to in-memory processing (10–100× faster).
- 🌊 **Apache Kafka** - Add real-time data ingestion for streaming analytics.
- 📊 **Apache Zeppelin** - Interactive notebooks for Pig and HiveQL queries.
- 🔍 **Elasticsearch + Kibana** - Search and visualise Hadoop outputs.
- ☁️ **AWS EMR / GCP Dataproc** - Deploy projects on cloud-managed Hadoop clusters.
- 🤖 **Apache Spark MLlib** - Machine learning on top of Hadoop datasets.

---

## 📄 License

```
MIT License

Copyright (c) 2026 Eshwar G

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.
```

---

<div align="center">

**⭐ Star this repository if you found it useful! ⭐**

*Built with 🐘 Apache Hadoop | 🐷 Apache Pig | 🐝 Apache Hive | 🐍 Python | ☕ Java*

</div>
