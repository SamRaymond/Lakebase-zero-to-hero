# Databricks notebook source
# MAGIC %pip install --upgrade -qqqq openai markdown
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Analysis Overview
# MAGIC
# MAGIC In this section, we will begin our analysis of the data to uncover meaningful insights and patterns that inform our research and decision-making.
# MAGIC
# MAGIC ## Approach
# MAGIC
# MAGIC - **Exploration:** Begin by exploring the dataset.
# MAGIC - **Statistical Analysis:** Perform various statistical analyses.
# MAGIC - **Visualization:** Visualize results to understand underlying trends.
# MAGIC
# MAGIC ## Systems Utilized
# MAGIC
# MAGIC We leverage both **Online Analytical Processing (OLAP)** and **Online Transaction Processing (OLTP)** systems, seamlessly integrated within this notebook:
# MAGIC
# MAGIC - **OLAP:** Enables complex queries and multidimensional analysis for a comprehensive data view.
# MAGIC - **OLTP:** Ensures efficient, accurate data transactions, maintaining dataset integrity and consistency.
# MAGIC
# MAGIC ## Benefits
# MAGIC
# MAGIC Combining OLAP and OLTP creates a robust, dynamic workflow that enhances analytical capabilities and streamlines the process. As PhD students, mastering these tools will significantly boost our research and analytical skills.

# COMMAND ----------

from pyspark.sql.functions import col, sum, avg, count, max, min

# Load the transactions table
table = spark.read.table("lakebase.public.transactions")

# Total sales per product
total_sales_per_product = table.groupBy("product").agg(sum(col("quantity") * col("price_per_unit")).alias("total_sales"))
display(total_sales_per_product)

# Average transaction value per customer
avg_transaction_value_per_customer = table.groupBy("customer_name").agg(avg(col("quantity") * col("price_per_unit")).alias("avg_transaction_value"))
display(avg_transaction_value_per_customer)

# Number of transactions per payment method
transactions_per_payment_method = table.groupBy("payment_method").agg(count("*").alias("transaction_count"))
display(transactions_per_payment_method)

# Top 5 cities by total sales
top_cities_by_sales = table.groupBy("city").agg(sum(col("quantity") * col("price_per_unit")).alias("total_sales")).orderBy(col("total_sales").desc()).limit(5)
display(top_cities_by_sales)

# Min, Max, and Average price per unit for each product
price_stats_per_product = table.groupBy("product").agg(
    min("price_per_unit").alias("min_price"),
    max("price_per_unit").alias("max_price"),
    avg("price_per_unit").alias("avg_price")
)
display(price_stats_per_product)

# COMMAND ----------

# MAGIC %md
# MAGIC # Creating a Sales Promo with GenAI
# MAGIC
# MAGIC Now, let's add a touch of innovation by incorporating Generative AI (GenAI) to craft a compelling sales promotion. This tutorial will guide you through the process, demonstrating how to leverage GenAI for effective marketing strategies.
# MAGIC
# MAGIC ### Step-by-Step Guide
# MAGIC
# MAGIC 1. **Data Collection:** Gather relevant data on customer preferences, purchase history, and market trends.
# MAGIC 2. **GenAI Integration:** Utilize GenAI tools to analyze the data and generate personalized promotional content.
# MAGIC 3. **Content Creation:** Develop engaging and targeted sales copy that resonates with your audience.
# MAGIC 4. **Implementation:** Deploy the promotional content across various channels to maximize reach and impact.
# MAGIC
# MAGIC ### Benefits of Using GenAI
# MAGIC
# MAGIC - **Personalization:** Tailor promotions to individual customer needs, increasing engagement and conversion rates.
# MAGIC - **Efficiency:** Automate content creation, saving time and resources.
# MAGIC - **Insightful Analysis:** Gain deeper insights into customer behavior and preferences, informing future marketing strategies.
# MAGIC
# MAGIC By following this approach, you'll not only enhance your marketing efforts but also gain valuable experience in utilizing advanced AI technologies. Let's dive in and create a sales promo that stands out!

# COMMAND ----------

from openai import OpenAI
import os

# How to get your Databricks token: https://docs.databricks.com/en/dev-tools/auth/pat.html
DATABRICKS_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

client = OpenAI(
  api_key=DATABRICKS_TOKEN,
  base_url="https://dbc-c0a0293f-50a4.cloud.databricks.com/serving-endpoints"
)

chat_completion = client.chat.completions.create(
  messages=[
  {
    "role": "system",
    "content": "You are a marketing AI assistant. You will be provided with structured sales analytics data. Based on this data, generate a creative and actionable sales promotion strategy to maximize sales."
  },
  {
    "role": "user",
    "content": f"""
Here is the latest sales analytics data:

Total sales per product:
{total_sales_per_product.limit(100).toPandas().to_dict(orient='records')}

Average transaction value per customer:
{avg_transaction_value_per_customer.limit(100).toPandas().to_dict(orient='records')}

Number of transactions per payment method:
{transactions_per_payment_method.limit(100).toPandas().to_dict(orient='records')}

Top 5 cities by total sales:
{top_cities_by_sales.limit(100).toPandas().to_dict(orient='records')}

Min, Max, and Average price per unit for each product:
{price_stats_per_product.limit(100).toPandas().to_dict(orient='records')}

Based on this data, generate a series of sales promos that users would see on the website. Be fun and creative! Make sure to link to the data points and estimate sales for each campagn. 
"""
  }
  ],
  model="databricks-llama-4-maverick",
  max_tokens=1024
)

import markdown
html_content = markdown.markdown(chat_completion.choices[0].message.content)
displayHTML(html_content)