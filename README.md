# 📰 News Data Pipeline

<div align="center">

🚀 **An end-to-end Data Engineering pipeline for automated news collection, processing, storage, and analytics.**

Designed using a modern **Medallion Architecture (Bronze, Silver, Gold)** to automate web scraping, real-time data ingestion, ETL processing, data warehousing, and monitoring.

![Python](https://img.shields.io/badge/Python-3.x-blue?logo=python)
![Apache Airflow](https://img.shields.io/badge/Apache-Airflow-red?logo=apacheairflow)
![Apache Kafka](https://img.shields.io/badge/Apache-Kafka-black?logo=apachekafka)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Database-blue?logo=postgresql)
![Docker](https://img.shields.io/badge/Docker-Container-blue?logo=docker)
![Grafana](https://img.shields.io/badge/Grafana-Monitoring-orange?logo=grafana)

</div>

---

# 📖 Overview

The **News Data Pipeline** is a modern Data Engineering project that automatically collects articles from multiple online news sources and processes them through a scalable ETL architecture.

The pipeline scrapes news websites, streams articles using **Apache Kafka**, stores raw data inside **MinIO** (Bronze Layer), transforms and enriches the data using **Apache Airflow** (Silver Layer), loads curated datasets into **PostgreSQL** (Gold Layer), and monitors the entire workflow through **Grafana dashboards**.

The project follows the **Medallion Architecture** to ensure high data quality, scalability, and maintainability.

---

# ✨ Features

- 📰 Automated web scraping from multiple news sources
- ⚡ Real-time streaming with Apache Kafka
- 🥉 Bronze Layer for raw data storage
- 🥈 Silver Layer for cleaning and enrichment
- 🥇 Gold Layer for analytics-ready datasets
- 🔍 Data validation and normalization
- 🌍 Language detection and metadata extraction
- 📦 Object storage using MinIO
- 🗄️ PostgreSQL Data Warehouse
- 🔄 Workflow orchestration with Apache Airflow
- 🐳 Dockerized deployment
- 📊 Real-time monitoring with Grafana
- 📈 Scalable ETL architecture

---

# 🏗️ Pipeline Architecture

```text
🌐 News Websites
        │
        ▼
📰 Python Web Scraper
        │
        ▼
⚡ Apache Kafka
        │
        ▼
🥉 Bronze Layer (MinIO)
        │
        ▼
🔄 Apache Airflow ETL
        │
        ▼
🥈 Silver Layer
(Data Cleaning & Enrichment)
        │
        ▼
🥇 Gold Layer
(PostgreSQL Data Warehouse)
        │
        ▼
📊 Grafana Dashboard
```

---

# 🛠️ Technologies

| Technology | Description |
|------------|-------------|
| 🐍 Python | Core application development |
| ⚡ Apache Kafka | Real-time data streaming |
| 🔄 Apache Airflow | Workflow orchestration |
| 🗄️ PostgreSQL | Data Warehouse |
| 📦 MinIO | Object Storage |
| 🐳 Docker & Docker Compose | Containerization |
| 📊 Grafana | Monitoring & Visualization |
| 🌐 BeautifulSoup | Web Scraping |
| 📡 Requests | HTTP Requests |

---

# 🚀 Project Objectives

- 📰 Automate news collection from multiple sources
- ⚡ Build a scalable real-time ingestion pipeline
- 🔄 Implement a complete ETL workflow
- 🏗️ Apply the Medallion Architecture
- 📦 Store structured and unstructured data efficiently
- 📊 Enable analytics-ready datasets
- 📈 Monitor pipeline health and performance
- 🐳 Ensure reproducibility through Docker

---

# 📂 Project Structure

```bash
News-Data-Pipeline/
│
├── airflow/
│   ├── dags/
│   └── plugins/
│
├── scraper/
├── kafka/
├── postgres/
├── minio/
├── grafana/
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

# 🎯 Learning Outcomes

This project provided hands-on experience with:

- 🏗️ Data Engineering pipelines
- ⚡ Apache Kafka streaming
- 🔄 Apache Airflow orchestration
- 📦 Object storage with MinIO
- 🗄️ PostgreSQL Data Warehousing
- 📊 Monitoring with Grafana
- 🐳 Docker & Docker Compose
- 🌐 Web scraping using BeautifulSoup
- 📈 ETL design and implementation
- 🥉🥈🥇 Medallion Architecture

---

# 👨‍💻 Author

**Hamza Zaidi**

🎓 Data Engineering & Artificial Intelligence Student

📍 Casablanca, Morocco

📧 zaidihamza1373@gmail.com

💼 LinkedIn: www.linkedin.com/in/hamza-zaidi-789b84262

⭐ If you found this project useful, consider giving it a **Star** on GitHub!
