# 📰 News Data Pipeline

An end-to-end data engineering project that automatically collects, processes, stores, and monitors news articles using a modern data pipeline architecture.

The pipeline scrapes articles from multiple news websites, ingests them into Apache Kafka, stores raw data in MinIO (Bronze Layer), performs data cleaning and enrichment through Apache Airflow (Silver Layer), loads curated data into PostgreSQL (Gold Layer), and visualizes pipeline metrics using Grafana.

## Features

- Automated web scraping from multiple news sources
- Kafka-based streaming ingestion
- Bronze / Silver / Gold data architecture
- Data cleaning and normalization
- Language detection and metadata enrichment
- Object storage with MinIO
- PostgreSQL Data Warehouse
- Workflow orchestration with Apache Airflow
- Dockerized deployment
- Monitoring dashboards with Grafana

## Tech Stack

- Apache Airflow
- Apache Kafka
- PostgreSQL
- MinIO
- Docker & Docker Compose
- Grafana
- Python
- BeautifulSoup
- Requests

## Pipeline Architecture
