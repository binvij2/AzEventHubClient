# Real-Time Streaming Demo with Azure Event Hub & Databricks DLT (Medallion Architecture)

This project demonstrates a real-time data pipeline leveraging **Azure Event Hub** for event streaming and **Databricks Delta Live Tables (DLT)** for ETL processing, all structured using the **Medallion architecture** (Bronze, Silver, Gold).

## 🛠️ Architecture Overview

- **Data Ingestion**: A custom CLI tool built with an **asynchronous Scala Spark client** simulates real-world streaming data and publishes it to **Azure Event Hub**.
- **Bronze Layer**: DLT reads raw events from Event Hub into a **materialized view**, capturing data exactly as received.
- **Silver Layer**: Cleansed and transformed data is processed from the Bronze layer — handling schema evolution, parsing, and filtering for quality.
- **Gold Layer**: Business-ready data is enriched and aggregated for use in downstream analytics, dashboards, and decision-making.

## 🔁 Key Highlights

- Real-time, scalable ingestion using Azure-native streaming infrastructure.
- Declarative ETL with DLT enabling reliable, incremental data processing.
- Clean separation of concerns using Medallion architecture for clarity and scalability.
- Easy to extend for advanced use cases like anomaly detection, ML feature pipelines, or BI dashboards.

## 🚀 Getting Started

1. Clone the repo.
2. Deploy required Event Hub and Databricks resources.
3. Run the CLI to simulate streaming input.
4. Observe how the data flows through the Bronze → Silver → Gold layers in real-time.

## 📌 Tech Stack

- Azure Event Hub  
- Databricks Delta Live Tables (DLT)  
- Scala client streaming data
- Medallion Architecture  

---

Feel free to fork, contribute, or adapt this demo for your own streaming analytics needs!
