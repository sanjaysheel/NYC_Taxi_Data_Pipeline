# NYC_Taxi_Data_Pipeline



Here’s the architecture diagram for the **NYC Taxi Data ETL Workflow** project. It includes all the components and data flow:



### **Data Flow**
- **Step 1**: Raw data is ingested into S3 (`/raw/`).
- **Step 2**: AWS Glue Crawlers detect schemas and populate the Glue Data Catalog.
- **Step 3**: AWS Glue ETL Jobs process raw data:
  - Cleaning.
  - Enrichment with metadata and weather data.
  - Aggregations and maintaining historical records (SCD Type 2).
- **Step 4**: Processed data is stored in S3 (`/processed/`) in optimized formats.
- **Step 5**: Athena queries data for insights.
- **Step 6**: QuickSight visualizes trends and performance metrics.

