# 🚀 End-to-End Data Engineering Pipeline

A fully automated, production-grade data engineering pipeline that ingests raw data 
from a MySQL source database, processes it through AWS services, loads it into 
Snowflake Data Warehouse, and visualizes it in Power BI — with zero manual intervention.

---

## 🏗️ Architecture

![Pipeline Architecture](architecture/pipeline_architecture.png)

```
MySQL  →  AWS S3  →  AWS Glue  →  Snowflake  →  Power BI
(Source)  (Data Lake)  (ETL)    (Warehouse)   (Dashboard)
```

---

## 🛠️ Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| Source DB | MySQL | Raw transactional data |
| Data Lake | AWS S3 | Raw data storage |
| ETL | AWS Glue | Extract, Transform, Load |
| Data Warehouse | Snowflake | Analysis-ready data |
| BI Tool | Power BI | Dashboards & Reporting |

---

## ⚙️ Pipeline Overview

1. **Ingestion** — Raw data is extracted from MySQL and landed into AWS S3 as the Data Lake
2. **Transformation** — AWS Glue ETL jobs clean, transform, and structure the data
3. **Loading** — Transformed data is loaded into Snowflake Data Warehouse
4. **Visualization** — Power BI connects to Snowflake for live, interactive dashboards
5. **Automation** — The entire pipeline runs automatically with no manual steps required

---

## 🚀 Getting Started

### Prerequisites
- AWS Account (S3 + Glue access)
- Snowflake Account
- MySQL Server
- Power BI Desktop
- Python 3.8+

### Installation

```bash
# Clone the repository
git clone https://github.com/your-username/end-to-end-data-engineering-pipeline.git

# Navigate into the project
cd end-to-end-data-engineering-pipeline

# Install dependencies
pip install -r requirements.txt
```

### Setup Steps

1. **MySQL** — Run `mysql/create_tables.sql` to set up the source schema
2. **AWS S3** — Create your S3 bucket and update the bucket name in `aws_s3/s3_upload.py`
3. **AWS Glue** — Upload `aws_glue/glue_etl_job.py` as a Glue job script
4. **Snowflake** — Run `snowflake/create_schema.sql` to set up the warehouse schema
5. **Power BI** — Open `powerbi/dashboard.pbix` and update the Snowflake connection

> See `docs/setup_guide.md` for a detailed step-by-step walkthrough.

---

## 📊 Project Demo

🎥 **Full Video Walkthrough** → [YouTube Link Here]

---

## 📄 License

This project is licensed under the MIT License.

---

## 🤝 Connect With Me

If you have any questions about this project or want to collaborate, feel free to reach out!

| Platform | Link |
|---|---|
| 📧 Email | [analysiswithdhiraj@gmail.com](mailto:analysiswithdhiraj@gmail.com) |
| 💼 LinkedIn | [linkedin.com/in/analystdhiraj](https://www.linkedin.com/in/analystdhiraj/) |

> ⭐ If you found this project helpful, please consider giving it a star — it really helps!
