# 🌤️ Big Data Project: Weather Impact on Urban Traffic Analysis

**Goal:** Design and implement a modern predictive data lake system to analyze how weather conditions affect urban traffic patterns in London.

**Team:** Member 1 - Infrastructure  
**Deadline:** December 14, 2025 at 12:00 midnight  
**Status:** ✅ Complete and Operational

---

## 📋 Project Overview

A smart city authority in London wants to understand how weather conditions (rain, temperature extremes, humidity, wind, visibility) influence traffic behavior and congestion levels. This project implements a complete data lake pipeline with:

- ✅ **Docker Infrastructure** - Container orchestration
- ✅ **MinIO** - Three-layer data lake (Bronze/Silver/Gold)
- ✅ **HDFS** - Distributed file system
- ✅ **Python Scripts** - Automated data processing
- ✅ **Monte Carlo Simulation** - Traffic risk prediction (Phase 5 - Member 2)
- ✅ **Factor Analysis** - Weather impact detection (Phase 6 - Member 3)

---

## 🏗️ Architecture

```
Raw Synthetic Data
       ↓
MinIO Bronze Layer (Raw CSV Data)
       ↓
Data Cleaning & Processing (Python)
       ↓
MinIO Silver Layer (Cleaned Parquet Data)
       ↓
Copy to HDFS (Distributed Storage Layer)
       ↓
Monte Carlo + Factor Analysis
       ↓
MinIO Gold Layer (Final Results & Reports)
```

---

## 🚀 Quick Start

### Prerequisites
- Docker Desktop installed and running
- Python 3.8+
- Git

### 1. Clone Repository
```bash
git clone https://github.com/RowanFayez/Big_data.git
cd Big_data/BigDataProject
```

### 2. Install Python Dependencies
```bash
pip install -r requirements.txt
```

### 3. Start Infrastructure
```bash
docker-compose up -d
```

Wait 10-15 seconds for containers to initialize.

### 4. Verify Containers
```bash
docker ps
```

You should see 3 running containers:
- `minio-server` - Object storage
- `hdfs-namenode` - HDFS master
- `hdfs-datanode` - HDFS worker

### 5. Run Complete Setup
```bash
python scripts/complete_setup.py
```

This script will:
- Upload raw data to Bronze bucket
- Upload cleaned data to Silver bucket
- Copy data to HDFS
- Upload analysis results to Gold bucket
- Verify all layers

### 6. Upload to HDFS (if needed)
```bash
python scripts/upload_to_hdfs.py
```

### 7. Verify Infrastructure
```bash
python scripts/verify_infrastructure.py
```

---

## 📂 Project Structure

```
BigDataProject/
├── docker-compose.yml           # Docker orchestration
├── requirements.txt             # Python dependencies
├── .gitignore                   # Git ignore rules
│
├── scripts/
│   ├── complete_setup.py        # Main automation script
│   ├── upload_to_hdfs.py        # HDFS upload utility
│   └── verify_infrastructure.py # Health check script
│
├── data/
│   ├── london_weather_dataset2.csv       # Raw weather data
│   ├── london_traffic_dataset2.csv       # Raw traffic data
│   ├── weather_cleaned.parquet           # Cleaned weather (Silver)
│   ├── traffic_cleaned.parquet           # Cleaned traffic (Silver)
│   ├── merged_data.parquet               # Merged dataset (Silver)
│   ├── factor_loadings.csv               # Factor analysis (Gold)
│   ├── final_factor_scores.csv           # Factor scores (Gold)
│   └── Factor Analysis Report.docx       # Analysis report (Gold)
│
├── test_list_files.py           # Bucket verification
├── test_ngrok_connection.py     # Remote access test
│
└── INFRASTRUCTURE_DATA_IMPORT_REPORT.md  # Technical documentation
```

---

## 🔧 Technology Stack

| Category | Tools |
|----------|-------|
| Infrastructure | Docker, MinIO, HDFS |
| Programming | Python 3.12 |
| Data Formats | CSV (Raw), Parquet (Cleaned) |
| Analysis | Monte Carlo Simulation, Factor Analysis |
| Libraries | minio, hdfs, pandas, pyarrow |

---

## 🗄️ Data Lake Layers

### Bronze Layer (Raw Data)
- **Purpose:** Store raw, unprocessed data
- **Format:** CSV
- **Contents:**
  - `london_weather_dataset2.csv` (0.57 MB)
  - `london_traffic_dataset2.csv` (0.43 MB)
- **Total:** 1.00 MB

### Silver Layer (Cleaned Data)
- **Purpose:** Store cleaned, validated, transformed data
- **Format:** Parquet with Snappy compression
- **Contents:**
  - `weather_cleaned.parquet` (0.18 MB)
  - `traffic_cleaned.parquet` (0.10 MB)
  - `merged_data.parquet` (0.20 MB)
- **Total:** 0.48 MB
- **Compression:** 52% size reduction from CSV

### Gold Layer (Analysis Results)
- **Purpose:** Store final results and reports
- **Format:** CSV, DOCX
- **Contents:**
  - `factor_loadings.csv` (0.00 MB)
  - `final_factor_scores_gold_layer_4_factors.csv` (0.65 MB)
  - `Factor Analysis on Urban Traffic and Weather Data.docx` (1.32 MB)
- **Total:** 1.97 MB

### HDFS (Distributed Storage)
- **Purpose:** Distributed file system for big data processing
- **Format:** Parquet
- **Structure:**
  ```
  /bigdata/
  ├── weather/weather_cleaned.parquet (190,833 bytes)
  ├── traffic/traffic_cleaned.parquet (104,503 bytes)
  └── merged/merged_data.parquet (210,713 bytes)
  ```
- **Total:** 506 KB

---

## 🌐 Access Points

### MinIO Console
- **URL:** http://localhost:9001
- **Username:** minioadmin
- **Password:** minioadmin123

### MinIO API
- **Endpoint:** http://localhost:9002
- **S3-Compatible:** Yes

### HDFS NameNode Web UI
- **URL:** http://localhost:9870
- **Features:** Browse files, monitor cluster, view DataNodes

### HDFS API
- **Endpoint:** http://localhost:9000
- **Protocol:** HDFS RPC

---

## 📊 Datasets

### Weather Dataset (~5000 records)
**Features:**
- Date & time
- City (London)
- Season
- Temperature (°C)
- Humidity (%)
- Rainfall (mm)
- Wind speed (km/h)
- Visibility (m)
- Weather condition
- Air pressure (hPa)

**Data Quality Issues (Cleaned):**
- ✅ Missing values handled
- ✅ Duplicates removed
- ✅ Outliers corrected
- ✅ Date formats standardized

### Traffic Dataset (~5000 records)
**Features:**
- Date & time
- City (London)
- Area/district
- Vehicle count
- Average speed (km/h)
- Accident count
- Congestion level
- Road condition
- Visibility (m)

**Data Quality Issues (Cleaned):**
- ✅ Negative speeds fixed
- ✅ Extreme vehicle counts handled
- ✅ Missing areas filled
- ✅ Congestion categories standardized

---

## 🔄 Automated Pipeline

The `complete_setup.py` script automates the entire pipeline:

**Phase 1:** Upload raw data to Bronze  
**Phase 2:** Upload cleaned data to Silver  
**Phase 3:** Copy data to HDFS  
**Phase 4:** Upload analysis results to Gold  
**Phase 5:** Verify all layers  

---

## 📸 Screenshots for Submission

### MinIO
1. Dashboard showing 3 buckets (bronze, silver, gold)
2. Bronze bucket with 2 CSV files
3. Silver bucket with 3 Parquet files
4. Gold bucket with 3 result files

### HDFS
1. NameNode overview page
2. DataNodes tab showing 1 live node
3. Browse filesystem: `/bigdata` directory structure
4. Browse filesystem: `/bigdata/weather` with parquet file
5. Browse filesystem: `/bigdata/traffic` with parquet file
6. Browse filesystem: `/bigdata/merged` with parquet file

### Docker
1. `docker ps` output showing all 3 containers running

---

## 🛠️ Troubleshooting

### Containers Not Starting
```bash
# Check Docker Desktop is running
docker ps

# Restart containers
docker-compose down
docker-compose up -d
```

### MinIO Connection Issues
- Verify port 9002 (API) and 9001 (Console) are not in use
- Check firewall settings
- Verify credentials: minioadmin/minioadmin123

### HDFS Connection Issues
```bash
# Check NameNode status
docker exec hdfs-namenode hdfs dfsadmin -report

# Check if safemode is off
docker exec hdfs-namenode hdfs dfsadmin -safemode get
```

### Port Conflicts
If port 9002 conflicts, modify `docker-compose.yml`:
```yaml
ports:
  - "9003:9000"  # Change 9002 to 9003
```
Then update Python scripts to use `localhost:9003`.

---

## 📝 Configuration Files

### docker-compose.yml
Defines 3 services:
- `minio` - Object storage (ports 9001, 9002)
- `namenode` - HDFS master (ports 9870, 9000)
- `datanode` - HDFS worker (port 9864)

### requirements.txt
Python dependencies:
- `minio` - MinIO Python SDK
- `hdfs` - HDFS Python client
- `python-dotenv` - Environment variables
- `pandas` - Data processing
- `pyarrow` - Parquet support

---

## ✅ Verification Checklist

### Infrastructure
- [x] Docker containers running (3/3)
- [x] MinIO accessible at localhost:9001
- [x] HDFS accessible at localhost:9870
- [x] No errors in logs

### Data
- [x] Bronze: 2 CSV files uploaded
- [x] Silver: 3 Parquet files uploaded
- [x] Gold: 3 result files uploaded
- [x] HDFS: 3 Parquet files in /bigdata

### Scripts
- [x] complete_setup.py runs without errors
- [x] upload_to_hdfs.py uploads successfully
- [x] verify_infrastructure.py passes all checks

---

## 🎯 Project Phases

### ✅ Phase 1: Infrastructure & Data Ingestion (Complete)
- Docker infrastructure deployed
- MinIO buckets created (bronze, silver, gold)
- Raw datasets uploaded to Bronze

### ✅ Phase 2: Data Cleaning (Complete)
- Data cleaned and validated
- Converted to Parquet format
- Uploaded to Silver layer

### ✅ Phase 3: HDFS Integration (Complete)
- HDFS cluster configured
- Cleaned data copied to HDFS
- Directory structure organized

### ✅ Phase 4: Dataset Merging (Complete)
- Weather and traffic datasets merged
- Merged dataset stored in Silver

### 🔄 Phase 5: Monte Carlo Simulation (Member 2)
- Simulate traffic behavior under weather conditions
- Calculate congestion probabilities
- Generate risk predictions

### 🔄 Phase 6: Factor Analysis (Member 3)
- Identify key weather drivers
- Extract latent factors
- Generate interpretation report

### 📊 Phase 7: Visualization Dashboard (Optional)
- Interactive web dashboard
- Display statistics and insights

---

## 📚 Documentation

- **[INFRASTRUCTURE_DATA_IMPORT_REPORT.md](INFRASTRUCTURE_DATA_IMPORT_REPORT.md)** - Complete technical report covering Docker, MinIO, HDFS setup, and data import process

---

## 🤝 Team Information

**Project Team:** 5-10 members  
**Member 1 Role:** Infrastructure Setup  
**Responsibilities:**
- Docker infrastructure deployment
- MinIO data lake configuration
- HDFS distributed storage integration
- Data pipeline automation
- Infrastructure documentation

---

## 📅 Timeline

- **Start Date:** December 5, 2025
- **Deadline:** December 14, 2025 at 12:00 midnight
- **Status:** Infrastructure Complete ✅

---

## 🔗 Useful Commands

### Docker Commands
```bash
# Start infrastructure
docker-compose up -d

# Stop infrastructure
docker-compose down

# View logs
docker-compose logs -f

# Check container status
docker ps

# Restart a specific container
docker-compose restart minio
```

### MinIO Commands (Python)
```python
from minio import Minio

client = Minio(
    "localhost:9002",
    access_key="minioadmin",
    secret_key="minioadmin123",
    secure=False
)

# List buckets
buckets = client.list_buckets()

# List objects
objects = client.list_objects("bronze")

# Download file
client.fget_object("silver", "weather_cleaned.parquet", "downloaded.parquet")
```

### HDFS Commands
```bash
# List files
docker exec hdfs-namenode hdfs dfs -ls /bigdata

# Check file size
docker exec hdfs-namenode hdfs dfs -du -h /bigdata

# Upload file
docker exec hdfs-namenode hdfs dfs -put /tmp/file.parquet /bigdata/

# Download file
docker exec hdfs-namenode hdfs dfs -get /bigdata/file.parquet /tmp/

# Check cluster health
docker exec hdfs-namenode hdfs dfsadmin -report
```

---

## 📞 Support

For issues or questions:
1. Check [INFRASTRUCTURE_DATA_IMPORT_REPORT.md](INFRASTRUCTURE_DATA_IMPORT_REPORT.md)
2. Run verification: `python scripts/verify_infrastructure.py`
3. Check Docker logs: `docker-compose logs`
4. Review HDFS status: http://localhost:9870

---

## 📄 License

This project is for educational purposes as part of the Big Data course.

---

## 🎉 Acknowledgments

- London Weather and Traffic data (synthetic datasets)
- Docker community for container images
- MinIO team for object storage solution
- Apache Hadoop team for HDFS
- Python community for amazing libraries

---

**Project Status:** ✅ Infrastructure Complete  
**Last Updated:** December 14, 2025  
**Version:** 1.0
