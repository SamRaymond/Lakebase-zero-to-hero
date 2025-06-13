# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC # Welcome to Lakebase!!
# MAGIC
# MAGIC ### What is Lakebase?
# MAGIC Databricks Lakebase is an online transaction processing (OLTP) database. These are a specialized type of database system designed to efficiently handle high volumes of real-time transactional data. Lakebase allows you to create an OLTP database on Databricks, and integrate OLTP workloads with your Lakehouse. This OLTP database enables you to create and manage databases stored in Databricks-managed storage.
# MAGIC
# MAGIC Using an OLTP database in conjunction with the Databricks platform significantly reduces application complexity. Lakebase is well integrated with Feature engineering and serving, SQL warehouses, and Databricks Apps. It is an simple and performant way to sync data between OLTP and online analytical processing (OLAP) workloads.
# MAGIC
# MAGIC Based on Postgres and fully integrated with the Databricks Data Intelligence Platform, Lakebase inherits several core platform capabilities, including:
# MAGIC - Simplified management: Leverages existing Databricks infrastructure to deploy instances with decoupled compute and storage, managed change data capture with Delta Lake, and support for multi-cloud deployments.
# MAGIC - Integrated artificial intelligence (AI) and machine learning (ML) capabilities: Supports feature and model serving, retrieval-augmented generation (RAG), and other AI and ML integrations.
# MAGIC - Integrated authentication and governance: Optionally, use Unity Catalog to enforce secure access to data.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating a Lakebase
# MAGIC
# MAGIC 1. Click on Compute and select `Database Instance` 
# MAGIC 2. Click on `Create database instance` 
# MAGIC <img src="media/image-7.png" width="1200"/>
# MAGIC 3. Name the instance (note that underscores are not permitted)
# MAGIC <img src="media/image-2.png" width="600"/>
# MAGIC 4. For now keep the defaults these can be updated later
# MAGIC 5. Click `Create` the instance will take a few mins to spin up 
# MAGIC <img src="media/image-7.png" width="1200"/>
# MAGIC 6. After the instance is ready, click on `catalog` and `Create catalog` 
# MAGIC <img src="media/image-6.png" width="600"/>
# MAGIC
# MAGIC 7. Name the catalog (underscores are allowed here). Leave the toggle off and use `databricks_postgres` as the PostgreSQL name. Click `create`
# MAGIC
# MAGIC 8. Once the catalog is ready, click on it and see the UC entry for the Lakebase data
# MAGIC <img src="media/image-8.png" width="1200"/>
# MAGIC
# MAGIC Now we're ready to start querying this PostgreSQL powered Lakebase! Head to `./01 - Lakebase - 101: psycopg3` to get started!
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Limitations and requirements
# MAGIC - A workspace allows a maximum of ten instances.
# MAGIC - Each instance supports up to 1000 concurrent connections.
# MAGIC - The logical size limit across all databases in an instance is 2 TB.
# MAGIC - Database instances are scoped to a single workspace. Users are able to see these tables in Catalog Explorer if they have the required Unity Catalog permissions from other workspaces attached to the same metastore, but they cannot access the table contents.