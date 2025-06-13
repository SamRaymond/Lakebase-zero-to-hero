# 🔄 Lakebase Quick Start Guide

This repo showcases how to combine **high-throughput transactional ingestion** using PostgreSQL-compatible OLTP with **generative AI insights** on fresh data using a Databricks Lakehouse.

It includes:
- A synthetic transaction generator using `Faker` and `psycopg3`
- A Lakehouse-backed OLTP setup using Databricks' PostgreSQL interface
- A GenAI agent that analyzes live transactions
- A background execution workflow for demo/production use

---

## 📁 Directory Structure

| File | Description |
|------|-------------|
| `00 - Setup a Lakebase.py` | Sets up the Lakebase instance on Databricks and generates database credentials |
| `01 - Lakebase - 101: psycopg3.py` | Connects to Lakehouse OLTP with `psycopg3`; tests simple inserts |
| `02 - Generate Transaction Data.py` | Synthetic data generator using `Faker`; simulates real-time transactions |
| `03 - GenAI + OLTP.py` | LLM agent that generates real-time insights or summaries from live transactions |

---

## 🚀 Quickstart (from a Databricks Notebook)

1. 🔑 **Run `00 - Setup a Lakebase.py`**  
   - Creates your PostgreSQL-compatible Lakebase instance  
   - Fetches and prints connection credentials

2. 🔗 **Test connection in `01 - Lakebase - 101: psycopg3.py`**  
   - Verifies connectivity with `psycopg3`
   - Creates the `transactions` table (if not exists)

3. 🧾 **Generate synthetic transactions with `02 - Generate Transaction Data.py`**  
   - Streams inserts into Lakehouse in real-time
   - TPS and row/s metrics are printed per batch

4. 🤖 **Run `03 - GenAI + OLTP.py`**  
   - Connects to Lakehouse
   - Periodically queries fresh data and generates summaries or strategy recommendations using a GenAI model

---

## 🧠 Dependencies

Make sure your Databricks cluster or job environment includes:

```bash
pip install psycopg[binary] faker openai
