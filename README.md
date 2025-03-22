# Parquet-in-Action

📌 Use Case: Customer Data Analytics
Scenario: A telecom company stores millions of customer records. The task is to analyze customers' average monthly usage and segment them.

🗂 Why Parquet?
Efficient for large datasets due to its columnar storage

Compression and encoding reduces storage cost

Faster read times for analytical queries

✅ Step-by-Step Execution in Databricks
🔹 Step 1: Upload or Mount the Raw CSV File
python
Copy
Edit
# Example: Mounting the data from an S3 bucket
df_raw = spark.read.csv("s3://telecom-bucket/customer_usage.csv", header=True, inferSchema=True)
df_raw.show(5)
🔹 Step 2: Data Cleansing & Transformation
Remove duplicates

Filter invalid records

Parse date fields

python
Copy
Edit
from pyspark.sql.functions import col, to_date

df_clean = df_raw.dropDuplicates() \
                 .filter(col("usage_gb").isNotNull()) \
                 .withColumn("billing_date", to_date(col("billing_date"), "yyyy-MM-dd"))

df_clean.show(5)
🔹 Step 3: Write the Cleaned Data to Parquet
python
Copy
Edit
df_clean.write.mode("overwrite").parquet("/mnt/clean_data/customer_usage_parquet")
📈 Databricks Value Add: Auto-optimizes Parquet compression (snappy) and partitions data for faster downstream queries.

🔹 Step 4: Read from Parquet for Analytics
python
Copy
Edit
df_parquet = spark.read.parquet("/mnt/clean_data/customer_usage_parquet")
df_parquet.createOrReplaceTempView("customer_usage")

# Run SQL for average usage
average_usage = spark.sql("""
    SELECT customer_id, AVG(usage_gb) as avg_usage
    FROM customer_usage
    GROUP BY customer_id
""")
average_usage.show(5)
✅ Value Add: Parquet + Spark SQL provides blazing-fast aggregation on large datasets.

🔹 Step 5: Further Analysis - Customer Segmentation
python
Copy
Edit
segmented = average_usage.withColumn(
    "segment",
    when(col("avg_usage") > 100, "High")
    .when(col("avg_usage") > 50, "Medium")
    .otherwise("Low")
)
segmented.show(5)
🔹 Step 6: Save Final Segmentation Output Back to Parquet
python
Copy
Edit
segmented.write.mode("overwrite").parquet("/mnt/output/customer_segments")
🎯 Databricks Advantages & Value Add in this Use Case
Feature	Value Add in Databricks
Delta Lake (optional)	Upgrades Parquet to ACID-compliant transactions with versioning, time travel, and schema enforcement.
Auto-scaling clusters	Handles billions of rows without manual tuning, optimized cost and compute.
Spark SQL + Parquet Combo	Blazing-fast querying with columnar optimization and partition pruning.
Data Lineage & Visualization	Native support for data lineage and dashboards for insights without switching tools.
Job Scheduling & Pipelines	Schedule this entire flow as a Databricks job or integrate into a production pipeline.
Cloud Storage Optimization	Direct integration with S3/ADLS for storage and compute separation, reducing costs.
ML-ready	Directly apply ML models on Parquet data for churn prediction or further analytics in the same notebook.
✅ Summary
Parquet + Databricks provides:

⚡ Performance: Faster queries on massive datasets

💰 Cost Efficiency: Compression + cloud storage optimization

🔄 Scalability: Handles real production workloads effortlessly

📊 Advanced Analytics: Built-in SQL, ML, and visualization tools

🔒 Data Governance Ready: Delta Lake compatibility for production-grade use

