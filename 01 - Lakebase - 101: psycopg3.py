# Databricks notebook source
# MAGIC %md
# MAGIC # 🐘 psycopg3 + Lakebase Intro (Databricks Python Workflow)
# MAGIC
# MAGIC This notebook shows how to use [`psycopg3`](https://www.psycopg.org/psycopg3/) to connect to a **Databricks Lakebase** PostgreSQL instance, run queries, and stream high-throughput inserts from Python.
# MAGIC
# MAGIC We'll use:
# MAGIC - `psycopg3`: modern, async‑ready PostgreSQL driver
# MAGIC - `psycopg_pool`: built-in connection pooling
# MAGIC - `Lakebase`: Databricks' serverless Postgres engine
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔧 What is `psycopg3`?
# MAGIC
# MAGIC - The official PostgreSQL driver for modern Python (3.7+)
# MAGIC - Successor to `psycopg2` (adds async support, COPY streaming, better pooling)
# MAGIC - Works with standard SQL, no ORM or abstraction layer
# MAGIC - Great for OLTP and high-throughput pipelines
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧠 Key Concepts
# MAGIC
# MAGIC | Concept     | What it is                                | Why it matters                        |
# MAGIC |-------------|--------------------------------------------|----------------------------------------|
# MAGIC | **Connection** | Link to your Lakebase database               | Needed to talk to Postgres              |
# MAGIC | **Cursor**     | Context for executing SQL + reading rows | One per query or transaction           |
# MAGIC | **Transaction** | Every operation runs in a TX by default  | Rollback-safe, consistent writes       |
# MAGIC | **COPY**       | Bulk load protocol in Postgres           | Orders of magnitude faster than `INSERT` |
# MAGIC | **Pool**       | Reuses open connections                  | Enables concurrency + efficiency       |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧪 What You'll Learn
# MAGIC
# MAGIC - How to generate credentials dynamically with the Databricks SDK
# MAGIC - How to use `psycopg3` to:
# MAGIC   - Connect to Lakebase
# MAGIC   - Run queries
# MAGIC   - Insert rows in batches
# MAGIC   - Bulk load millions of rows via `COPY`
# MAGIC - How to track **TPS** and **rows/sec** during ingest
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC

# COMMAND ----------

## Setup Requirements
%pip install --upgrade databricks-sdk
%pip install "psycopg[binary,pool]"
%restart_python

# COMMAND ----------

from databricks.sdk import WorkspaceClient
import uuid

# Initialize the WorkspaceClient to interact with Databricks
w = WorkspaceClient()

# Define the database instance name
instance_name = "my-lakebase"

# Get the current user
current_username = w.current_user.me().user_name

# Retrieve the database instance details
instance = w.database.get_database_instance(name=instance_name)

# Generate database credentials for the specified instance
cred = w.database.generate_database_credential(request_id=str(uuid.uuid4()), instance_names=[instance_name])
password = cred.token

# COMMAND ----------

# Table configuration
table_name = "public.lakebase_data"

# Insert range configuration
insert_start_id = 1
insert_end_id = 5000

# Select query limit
select_limit_rows = 50

# Initial setup queries: create table and insert sample data
initial_setup_sql = f"""
CREATE TABLE IF NOT EXISTS {table_name} (id INT, name TEXT);
INSERT INTO {table_name} (id, name) VALUES (1, 'John'), (2, 'Jane');
SELECT * FROM {table_name};
"""

# Insert data query: bulk insert using generate_series
bulk_insert_sql = f"""
CREATE TABLE IF NOT EXISTS {table_name} (id INT, name TEXT);
INSERT INTO {table_name} (id, name)
SELECT i, 'Lake ' || i
FROM generate_series({insert_start_id}, {insert_end_id}) AS s(i);
"""

# Read data query: select limited rows from the table
select_data_sql = f"""
SELECT * FROM {table_name}
LIMIT {select_limit_rows};
"""

# COMMAND ----------

from databricks.sdk import WorkspaceClient
import uuid

import psycopg
import string
from psycopg_pool import ConnectionPool

w = WorkspaceClient()

class CustomConnection(psycopg.Connection):
    global w
    def __init__(self, *args, **kwargs):
        # Call the parent class constructor
        super().__init__(*args, **kwargs)

    @classmethod
    def connect(cls, conninfo='', **kwargs):
        # Append the new password to kwargs
        instance = w.database.get_database_instance(name=instance_name)
        cred = w.database.generate_database_credential(request_id=str(uuid.uuid4()), instance_names=[host])
        kwargs['password'] = cred.token

        # Call the superclass's connect method with updated kwargs
        return super().connect(conninfo, **kwargs)


instance = w.database.get_database_instance(name=instance_name)
host = instance.read_write_dns
port = 5432
n_threads = 4
database = "databricks_postgres"

# Table configuration
table_name = "public.lakebase_data"

# Insert range configuration
insert_start_id = 1
insert_end_id = 5000

# Create a psycopg3 connection pool (thread-safe, efficient)
pool = ConnectionPool(
    conninfo=f"dbname={database} user={current_username} host={host} password={password}",
    min_size=4,
    max_size=n_threads,
    open=True
)

with pool.connection() as conn:
    with conn.cursor() as cursor:
        cursor.execute(initial_setup_sql)
        cursor.execute(bulk_insert_sql)
        cursor.execute(select_data_sql)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 OLTP vs OLAP: Managing Massive Data Volumes OLTP vs OLAP: Managing Massive Data VolumesTP vs OLAP: Handling Massive Data Volumes
# MAGIC
# MAGIC When working with large-scale data, it's important to choose the right system for your workload. **OLTP (Online Transaction Processing)** systems are designed to efficiently handle massive numbers of small, fast transactions—think thousands or millions of inserts, updates, or reads per second. In contrast, **OLAP (Online Analytical Processing)** systems excel at complex analytical queries over large datasets, but are not optimized for high-throughput transactional workloads.
# MAGIC
# MAGIC In this section, we'll focus on OLTP and demonstrate how Databricks Lakebase, combined with efficient drivers like `psycopg3`, can ingest and process huge volumes of data in real time. 

# COMMAND ----------

import threading, time, random
from psycopg_pool import ConnectionPool

# Number of threads to use for parallel processing
n_threads = 64         # Try 8, 16, 32, 64 to saturate

# Number of rows to insert per transaction
batch_size = 100       

# Total number of batches each thread will process
total_batches = 500    

# Initialize Databricks Workspace Client
w = WorkspaceClient()

# Database connection details
instance = w.database.get_database_instance(name=instance_name)
host = instance.read_write_dns
database = "databricks_postgres"

# Generate database credentials
cred = w.database.generate_database_credential(request_id=str(uuid.uuid4()), instance_names=[instance_name])
password = cred.token

# Create a connection pool with specified parameters
pool = ConnectionPool(
    conninfo=f"dbname={database} user={current_username} host={host} password={password}",
    min_size=4,
    max_size=n_threads,
    open=True
)

# Worker function to be executed by each thread
def worker(thread_idx, results):
    count = 0
    with pool.connection() as conn:
        with conn.cursor() as cur:
            for _ in range(total_batches):
                # Generate a batch of rows to insert
                rows = [(random.randint(1, int(1e9)), f"Lake_{thread_idx}_{i}") for i in range(batch_size)]
                # Insert rows into the table, ignoring conflicts
                cur.executemany("INSERT INTO public.lakebase_sjr2 (id, name) VALUES (%s, %s) ON CONFLICT DO NOTHING", rows)
                conn.commit()
                count += batch_size
    results[thread_idx] = count

# Ensure the target table exists
with pool.connection() as conn:
    with conn.cursor() as cur:
        cur.execute("CREATE TABLE IF NOT EXISTS public.lakebase_sjr2 (id INT PRIMARY KEY, name TEXT);")

# List to hold thread objects
threads = []

# List to store results from each thread
results = [0] * n_threads

# Record the start time
start = time.time()

# Create and start threads
for i in range(n_threads):
    t = threading.Thread(target=worker, args=(i, results))
    threads.append(t)
    t.start()

# Wait for all threads to complete
for t in threads:
    t.join()

# Calculate elapsed time
elapsed = time.time() - start

# Calculate total rows inserted and total transactions
total_rows = sum(results)
total_txns = n_threads * total_batches

# Print performance metrics
print(f"Inserted {total_rows:,} rows via {total_txns:,} transactions in {elapsed:.2f} sec")
print(f"TPS: {total_txns/elapsed:.1f}   Rows/sec: {total_rows/elapsed:.0f}")

# COMMAND ----------

# MAGIC %md
# MAGIC Now that we have hit out OLTP engine hard, head over to the `Metrics` section on the endpoint to see the load on the Lakebase:
# MAGIC
# MAGIC ![](./media/image-10.png)
# MAGIC