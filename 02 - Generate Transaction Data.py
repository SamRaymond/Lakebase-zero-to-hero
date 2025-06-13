# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC
# MAGIC In this notebook, we will generate synthetic transaction data using the Faker library and then analyze this data. This approach is quite novel as it leverages both the OLTP and OLAP capabilities of Databricks, utilizing the Lakehouse architecture.
# MAGIC
# MAGIC ## Objectives
# MAGIC
# MAGIC 1. **Data Generation:** Use Faker to create realistic transaction data.
# MAGIC 2. **Data Storage:** Store the generated data in a Databricks Lakehouse, which combines the best features of data lakes and data warehouses.
# MAGIC 3. **Data Analysis:** Perform both OLTP and OLAP operations to analyze the data.
# MAGIC
# MAGIC ## Steps
# MAGIC
# MAGIC 1. **Setup Environment:** Install and import necessary libraries.
# MAGIC 2. **Generate Data:** Use Faker to create a dataset of transactions.
# MAGIC 3. **Store Data:** Save the generated data in the Lakehouse.
# MAGIC 4. **Analyze Data:** Conduct various analyses using OLTP and OLAP techniques.
# MAGIC 5. **Visualize Results:** Create visualizations to interpret the data.
# MAGIC
# MAGIC By the end of this tutorial, you will have a comprehensive understanding of how to generate, store, and analyze data using Databricks' Lakehouse platform, enhancing your data engineering and analytical skills.

# COMMAND ----------

# MAGIC %pip install --upgrade -qqqq databricks-sdk faker "psycopg[binary,pool]" openai markdown
# MAGIC %restart_python

# COMMAND ----------

from databricks.sdk import WorkspaceClient
import uuid

w = WorkspaceClient()

instance_name = "my-lakebase"
instance = w.database.get_database_instance(name=instance_name)
cred = w.database.generate_database_credential(request_id=str(uuid.uuid4()), instance_names=[instance_name])
password = cred.token
username = current_username = w.current_user.me().user_name
host = instance.read_write_dns
port = 5432
database = "databricks_postgres"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Why Use Faker for Data Generation?
# MAGIC
# MAGIC The **Faker** library is a powerful tool for generating synthetic data that closely mimics real-world datasets. In this section, we use Faker to create realistic transaction records, which is essential for several reasons:
# MAGIC
# MAGIC - **Safe Experimentation:** It allows us to work with data that resembles actual transactions without exposing sensitive or confidential information.
# MAGIC - **Reproducibility:** Synthetic data ensures that experiments and analyses can be easily reproduced and shared.
# MAGIC - **Scalability:** Faker can quickly generate large volumes of data, making it ideal for testing data pipelines, analytics, and machine learning models.
# MAGIC
# MAGIC By leveraging Faker, we can simulate a variety of transaction scenarios, enabling robust testing and analysis within the Databricks Lakehouse environment.

# COMMAND ----------

import random
import time
from datetime import datetime
from faker import Faker
import psycopg
from psycopg_pool import ConnectionPool

TABLE_NAME = "public.transactions"

n_threads = 16

# How many transactions to generate per batch?
BATCH_SIZE = 10

# Delay between batches (seconds)
BATCH_INTERVAL = 0

# ------------------------------------------------------------------------------
# INIT: Faker & DB Pool
# ------------------------------------------------------------------------------

faker = Faker()

# Create a psycopg3 connection pool (thread-safe, efficient)
pool = ConnectionPool(
    conninfo=f"dbname={database} user={username} host={host} password={password}",
    min_size=4,
    max_size=n_threads,
    open=True
)


# ------------------------------------------------------------------------------
# FUNCTION: Generate a single transaction (with optional promo bias)
# ------------------------------------------------------------------------------

def generate_transaction(promo_items=("Phone", "Laptop")):
    product_list = ["Shoes", "Shirt", "Phone", "Laptop", "Book"]
    weights = [1, 1, 5 if "Phone" in promo_items else 1,
                    5 if "Laptop" in promo_items else 1, 1]

    product = random.choices(product_list, weights=weights, k=1)[0]

    return {
        "transaction_id": faker.uuid4(),
        "timestamp": datetime.utcnow().isoformat(),
        "customer_name": faker.name(),
        "email": faker.email(),
        "product": product,
        "quantity": random.randint(1, 5),
        "price_per_unit": round(random.uniform(10.0, 500.0), 2),
        "payment_method": random.choice(["Credit Card", "PayPal", "Crypto", "Gift Card"]),
        "city": faker.city(),
        "country": faker.country()
    }

# ------------------------------------------------------------------------------
# FUNCTION: Insert batch of transactions into DB
# ------------------------------------------------------------------------------

def insert_transactions(transactions):
    with pool.connection() as conn:
        with conn.cursor() as cur:
            # Step 1: Ensure table exists
            cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                transaction_id VARCHAR(36),
                timestamp TIMESTAMP NOT NULL,
                customer_name VARCHAR(255),
                email VARCHAR(255),
                product VARCHAR(255),
                quantity INT,
                price_per_unit DECIMAL(10, 2),
                payment_method VARCHAR(255),
                city VARCHAR(255),
                country VARCHAR(255)
            );
            """)

            # Step 2: Insert batch of transactions
            insert_query = f"""
            INSERT INTO {TABLE_NAME} (
                transaction_id, timestamp, customer_name, email,
                product, quantity, price_per_unit,
                payment_method, city, country
            ) VALUES (
                %(transaction_id)s, %(timestamp)s, %(customer_name)s, %(email)s,
                %(product)s, %(quantity)s, %(price_per_unit)s,
                %(payment_method)s, %(city)s, %(country)s
            );
            """
            cur.executemany(insert_query, transactions)
        conn.commit()

# ------------------------------------------------------------------------------
# MAIN LOOP
# ------------------------------------------------------------------------------

def main():
    print("🔄 Starting transaction generator...")
    end_time = time.time() + 15 * 60  # Run for 15 minutes

    try:
        while time.time() < end_time:
            batch = [generate_transaction() for _ in range(BATCH_SIZE)]
            insert_transactions(batch)
            print(f"✅ Inserted {len(batch)} transactions at {datetime.utcnow().isoformat()}")
            time.sleep(BATCH_INTERVAL)

    except KeyboardInterrupt:
        print("\n🛑 Generator stopped manually.")

    finally:
        pool.close()
        print("🧹 Connection pool closed.")

# ------------------------------------------------------------------------------
# ENTRY POINT
# ------------------------------------------------------------------------------

if __name__ == "__main__":
    main()

# COMMAND ----------

import random
import time
from datetime import datetime
from faker import Faker
import psycopg
from psycopg_pool import ConnectionPool

TABLE_NAME = "public.transactions"

n_threads = 16

# How many transactions to generate per batch?
BATCH_SIZE = 10

# Delay between batches (seconds)
BATCH_INTERVAL = 0

# ------------------------------------------------------------------------------
# INIT: Faker & DB Pool
# ------------------------------------------------------------------------------

faker = Faker()

# Create a psycopg3 connection pool (thread-safe, efficient)
pool = ConnectionPool(
    conninfo=f"dbname={database} user={username} host={host} password={password}",
    min_size=4,
    max_size=n_threads,
    open=True
)


# ------------------------------------------------------------------------------
# FUNCTION: Generate a single transaction (with optional promo bias)
# ------------------------------------------------------------------------------

def generate_transaction(promo_items=("Phone", "Laptop")):
    product_list = ["Shoes", "Shirt", "Phone", "Laptop", "Book"]
    weights = [1, 1, 5 if "Phone" in promo_items else 1,
                    5 if "Laptop" in promo_items else 1, 1]

    product = random.choices(product_list, weights=weights, k=1)[0]

    return {
        "transaction_id": faker.uuid4(),
        "timestamp": datetime.utcnow().isoformat(),
        "customer_name": faker.name(),
        "email": faker.email(),
        "product": product,
        "quantity": random.randint(1, 5),
        "price_per_unit": round(random.uniform(10.0, 500.0), 2),
        "payment_method": random.choice(["Credit Card", "PayPal", "Crypto", "Gift Card"]),
        "city": faker.city(),
        "country": faker.country()
    }

# ------------------------------------------------------------------------------
# FUNCTION: Insert batch of transactions into DB
# ------------------------------------------------------------------------------

def insert_transactions(transactions):
    with pool.connection() as conn:
        with conn.cursor() as cur:
            # Step 1: Ensure table exists
            cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                transaction_id VARCHAR(36),
                timestamp TIMESTAMP NOT NULL,
                customer_name VARCHAR(255),
                email VARCHAR(255),
                product VARCHAR(255),
                quantity INT,
                price_per_unit DECIMAL(10, 2),
                payment_method VARCHAR(255),
                city VARCHAR(255),
                country VARCHAR(255)
            );
            """)

            # Step 2: Insert batch of transactions
            insert_query = f"""
            INSERT INTO {TABLE_NAME} (
                transaction_id, timestamp, customer_name, email,
                product, quantity, price_per_unit,
                payment_method, city, country
            ) VALUES (
                %(transaction_id)s, %(timestamp)s, %(customer_name)s, %(email)s,
                %(product)s, %(quantity)s, %(price_per_unit)s,
                %(payment_method)s, %(city)s, %(country)s
            );
            """
            cur.executemany(insert_query, transactions)
        conn.commit()

# ------------------------------------------------------------------------------
# MAIN LOOP
# ------------------------------------------------------------------------------

def main():
    print("🔄 Starting transaction generator...")
    end_time = time.time() + 7 * 60  # Run for 7 minutes

    try:
        while time.time() < end_time:
            batch = [generate_transaction() for _ in range(BATCH_SIZE)]
            insert_transactions(batch)
            print(f"✅ Inserted {len(batch)} transactions at {datetime.utcnow().isoformat()}")
            time.sleep(BATCH_INTERVAL)

    except KeyboardInterrupt:
        print("\n🛑 Generator stopped manually.")

    finally:
        pool.close()
        print("🧹 Connection pool closed.")

# ------------------------------------------------------------------------------
# ENTRY POINT
# ------------------------------------------------------------------------------

if __name__ == "__main__":
    main()

# COMMAND ----------

# MAGIC %md
# MAGIC ![](./media/image.png)