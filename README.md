# Fraud Detection in Financial Transactions with Time Windowing  

## Project Overview  
This project aims to detect fraudulent transactions in financial data using **Databricks, Apache Spark**, and **time-based windowing techniques**. The approach includes **data preprocessing, feature engineering, and anomaly detection** to identify suspicious transaction patterns.  

## Technologies Used  
- 🟢 **Databricks** – Cloud-based analytics platform  
- 🔥 **Apache Spark (PySpark)** – Distributed data processing  
- ☁ **AWS S3** – Cloud storage for transaction data  
- 📂 **Delta Lake** – Optimized storage format for data operations  
- 🐍 **Python** – Data transformation and analysis  

## Workflow  

### 🔹 1. Mount and Unmount AWS S3 Bucket  
- Use `dbutils.fs.mount` to connect to the **AWS S3 bucket** and access transaction data.  
- Perform **unmount and remount operations** for efficient storage handling.  

### 🔹 2. Data Ingestion  
- Load financial transaction data from **AWS S3** into a **Spark DataFrame**.  
- Display the **schema** and inspect the dataset.  

### 🔹 3. Data Cleaning  
- **Filter out invalid transactions**, such as negative amounts.  
- **Handle missing values** to maintain data consistency.  

### 🔹 4. Feature Engineering using Time-Based Windowing  
- **7-day total**: Computes the total transaction amount per customer in the last **7 days**.  
- **30-day moving average**: Calculates the rolling **average transaction value** over **30 days**.  
- **Previous transaction comparison**: Uses the `lag()` function to detect sudden **spikes** in transaction amounts.  

### 🔹 5. Fraud Detection Logic  
- **Flag transactions** where the current amount is more than **3 times the 30-day moving average** as **suspicious**.  

### 🔹 6. Data Storage  
- Save the **enriched dataset** into a **Delta table** for optimized querying.  
- Write the **transformed data** back to **AWS S3** in **Delta format**.  

## Results & Insights  
✅ Efficiently detects **anomalies** in transactions based on **historical trends and sudden spikes**.  
✅ **Time-windowing techniques** help identify **suspicious financial activities**.  
✅ Data is stored in **Delta format**, ensuring **fast querying and further analysis**.  

## Future Enhancements  
🚀 Implement **real-time fraud detection** using **Apache Kafka and Spark Streaming**.  
🤖 Use **machine learning models** (e.g., **Random Forest, XGBoost**) for **better fraud classification**.  
🔔 Integrate **alerts/notifications** for **flagged transactions**.  
