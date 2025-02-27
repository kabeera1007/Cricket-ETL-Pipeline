# **Live streaming Cricket Data Pipeline: AWS, Snowflake, Databricks & Power BI**

https://github.com/kabeera1007/Cricket-ETL-Pipeline/blob/main/flowchar_etl_cric.png?raw=true

## **1. Introduction**
This project automates the extraction, transformation, and visualization of live cricket data from **CricLive** and **ESPNcricinfo** APIs. It utilizes **AWS Lambda** to fetch data, **AWS Glue** (or optionally **Databricks**) to process it, and **Snowflake** for storage via **Snowpipe**. The workflow is managed using **AWS Step Functions & EventBridge**, and the final insights are visualized in **Power BI**. This ensures **real-time, scalable, and automated data analytics**.

---

## **2. Tools & Technologies Used**
### **AWS Services**
- **Lambda** - Fetches data from APIs and stores it in S3.
- **S3** - Stores raw and transformed data.
- **Glue** - Cleans and structures data (optional: Databricks).
- **Step Functions** - Orchestrates the data pipeline.
- **EventBridge** - Triggers automated execution.

### **Snowflake**
- **Database & Schema** - Organizes cricket data.
- **Snowpipe** - Automates data ingestion from S3.

### **Databricks (Optional)**
- **PySpark** - Alternative for data transformation.

### **Other Tools**
- **Python** - Used in `extract.py` and `transform.py`.
- **SQL** - Used in `load.txt` for Snowflake schema & Snowpipe.
- **Power BI** - Connects to Snowflake for visualization.

---

## **3. Step-by-Step Guide**
### **Step 1: Extract Data using AWS Lambda**
####  **What You Need**
- API Access to CricLive & ESPNcricinfo.
- AWS Lambda function in Python (`extract.py`).
- AWS IAM Role with **S3 write permissions**.

#### **How to Do It**
1. **Create an AWS Lambda function** in Python.
2. **Write `extract.py` script**:
   - Sources: Data is collected via an API (Cricket Live Line API) and web scraping (ESPN Cricinfo).
   - Convert response into JSON.
   - Save JSON to an S3 bucket.
3. **Set up EventBridge** to trigger Lambda every few minutes.

---

### **Step 2: Transform Data using AWS Glue (or Databricks)**
####  **What You Need**
- AWS Glue job in Python (`transform.py`).
- AWS IAM Role with **S3 & Glue permissions**.

####  **How to Do It (AWS Glue)**
1. **Create a Glue Job**:
   - Read raw JSON data from S3.
   - Clean, structure, and format the data.
   - Convert it into **Parquet or CSV**. we will use csv
   - Save transformed data back to S3.

####  **Optional: Using Databricks for Transformation**
If using **Databricks instead of Glue**, follow these steps:
1. **Upload Databricks Notebook** to your Databricks workspace.
2. **Use PySpark** to:
   - Read data from S3.
   - Transform and clean data.
   - Write the final dataset to Snowflake or Delta Lake.
3. **Schedule the Notebook** to run periodically.

---

### **Step 3: Load Data into Snowflake**
####  **What You Need**
- Snowflake database & schema (`load.txt`).
- AWS IAM Role with **S3 read access**.

####  **How to Do It**
1. **Use SQL script (`load.txt`)** to:
   - Create the database & schema in Snowflake.
   - Define tables for cricket data.
   - Set up Snowpipe for auto-ingestion.
2. **Configure Snowpipe**:
   - Define **external stage** for S3.
   - Use `COPY INTO` command to load transformed data.

---

### **Step 4: Orchestrate Workflow using AWS Step Functions**
####  **What You Need**
- AWS Step Functions
- EventBridge trigger

####  **How to Do It**
1. **Create a Step Function**:
   - Triggers AWS Glue (or Databricks job).
   - Calls Snowpipe to load data.
2. **Use EventBridge**:
   - Schedule function execution after Lambda runs.

---

### **Step 5: Visualize in Power BI**
####  **What You Need**
- Power BI
- Snowflake Connector

####  **How to Do It**
1. **Connect Power BI to Snowflake** using **ODBC/JDBC**.
2. **Build Dashboards**:
   - Create match analytics, player stats, and performance trends.

---

## **4. Summary of Key Components**
| **Component**     | **Technology Used**          |
|------------------|----------------------------|
| **Extraction**  | AWS Lambda, S3             |
| **Transformation** | AWS Glue / Databricks     |
| **Orchestration** | Step Functions, EventBridge |
| **Storage**      | Snowflake, S3              |
| **Ingestion**   | Snowpipe                    |
| **Visualization** | Power BI                   |

---

## **5. Optional: Using Databricks Instead of Glue**
If using **Databricks**, follow these adjustments:
- Replace **AWS Glue** with a **Databricks Notebook**.
- Use **Databricks Jobs** instead of Step Functions.

---
