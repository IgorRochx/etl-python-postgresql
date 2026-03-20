# ETL Python + PostgreSQL

A simple ETL (Extract, Transform, Load) pipeline built with Python, Pandas, and PostgreSQL.

## Description

This project reads a CSV file with sales data, cleans and transforms it using Pandas, and loads it into a PostgreSQL database with three related tables.

## ETL Flow

CSV → Python/Pandas (transform) → PostgreSQL (load)

## Technologies

- Python 3.13
- Pandas
- Psycopg 3
- PostgreSQL
- python-dotenv

## Database Structure

- **TB_Cliente** — customer data
- **TB_Veiculo** — vehicle data
- **TB_Venda** — sales data (references TB_Cliente and TB_Veiculo)

## Setup

1. Clone the repository
2. Install dependencies: pip install pandas psycopg python-dotenv
3. Create a .env file with your database credentials
4. Run: python project_cvs_psql.py

## Data Cleaning

The raw CSV contains intentional data quality issues handled during transform:

- Empty valor_venda → removed
- Invalid valor_venda ("ERRO") → remo