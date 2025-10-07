---
title: "Netflix Data Analysis | DBT (Data Build Tool) | End-To-End Project"
author: "Laxmi S Kallimath"
output: html_document
---

# About the Project

This project is an end-to-end **data engineering pipeline** built for **Netflix movie data analysis**.  
The data is first uploaded to **Amazon S3** and then loaded into **Snowflake**.  
Using **dbt (data build tool)**, the data is transformed with key dbt concepts such as **snapshots**, **modeling**, and **testing**.  

The project follows a **modern ELT (Extract, Load, Transform)** approach, with dbt at the core, to ensure scalable and efficient data transformation.  
It demonstrates how to design and build a **complete data pipeline** that prepares movie data for analytics and insights.

---

# STEP 1: Project Architecture

The project follows an **ELT pipeline** using **Amazon S3**, **Snowflake**, and **dbt**.  
Processed data is then connected to BI tools such as **Looker Studio**, **Power BI**, or **Tableau**.

## Layers

1. **Raw Layer (Extract & Load Layer)**  
   - Data uploaded into Amazon S3 and loaded into Snowflake (Raw Zone).  
   - **Purpose:** Keep unmodified source data safe for reference and recovery.

2. **Staging Layer**  
   - Prepares raw data for transformation.  
   - **Purpose:** Acts as an intermediate layer to prevent accidental changes.

3. **Transform & Development Layer**  
   - dbt performs transformations, testing, and orchestration.  
   - **Purpose:** Build business-ready datasets optimized for analytics.

4. **Serving Layer**  
   - Final data connected to BI tools for visualization.  
   - **Purpose:** Enable insights and data-driven decision-making.

---

# STEP 2: Prerequisites

- **SQL Basics:** SELECT, CTEs, joins, filters, aggregations  
- **Snowflake Basics:** Databases, schemas, tables  
- **Data Engineering:** Understanding ELT pipelines, raw/staging layers  

---

# STEP 3: What is DBT?

**dbt (data build tool)** is a transformation framework that converts raw data into analytics-ready datasets **inside the data warehouse**.

**Key Features:**
- SQL-based transformation  
- Modular and version-controlled  
- Built-in testing & documentation  
- Supports Snowflake, BigQuery, Redshift, Databricks  

**Why dbt?**
- Replaces manual ETL scripts  
- Adds testing, reusability, and dependency management  
- Improves collaboration and data quality  

---

# STEP 4: Foundational Concepts

| Concept | Description |
|----------|-------------|
| **Data Warehouse** | Central repository for structured/semi-structured data |
| **Data Lake** | Stores raw data (e.g., S3) |
| **Data Lakehouse** | Combines warehouse & lake benefits |
| **ETL vs ELT** | dbt uses ELT: Extract & Load first, then Transform |

---

# STEP 5: Real Project Execution

**Dataset Used:** [MovieLens 20M Dataset](https://grouplens.org/datasets/movielens/20m/)  

**Contains:**
- Ratings (~20M)  
- Users (~138K)  
- Movies (~20K)  
- Tags (~465K)  

**Files:**  
`ratings.csv`, `tags.csv`, `movies.csv`, `links.csv`, `genome-scores.csv`, `genome-tags.csv`

---

## Upload Data to Amazon S3

1. Create an S3 bucket  
2. Upload all CSV files  
3. Example path:  
   `s3://movielens-20m-laxmi-2025/raw-data/ratings.csv`

---

# STEP 6: Snowflake Setup

```sql
USE ROLE ACCOUNTADMIN;
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH;
CREATE DATABASE IF NOT EXISTS MOVIELENS;
CREATE SCHEMA IF NOT EXISTS MOVIELENS.RAW;

CREATE ROLE IF NOT EXISTS TRANSFORM;
GRANT OPERATE ON WAREHOUSE COMPUTE_WH TO ROLE TRANSFORM;

CREATE USER IF NOT EXISTS dbt 
PASSWORD='dbtPassword123' 
DEFAULT_ROLE=TRANSFORM 
DEFAULT_WAREHOUSE='COMPUTE_WH';

GRANT ROLE TRANSFORM TO USER dbt;

CREATE OR REPLACE STAGE NETFLIXSTAGE
URL='s3://dbtnetflixdataset'
CREDENTIALS=(AWS_KEY_ID='your_key' AWS_SECRET_KEY='your_secret_key');

CREATE OR REPLACE TABLE raw_movies (
  movieId INTEGER, 
  title STRING, 
  genres STRING
);

COPY INTO raw_movies 
FROM '@NETFLIXSTAGE/movies.csv' 
FILE_FORMAT=(TYPE='CSV' SKIP_HEADER=1 FIELD_OPTIONALLY_ENCLOSED_BY='"');
```

Repeat for other files: `raw_ratings`, `raw_tags`, `raw_genome_scores`, `raw_genome_tags`, `raw_links`.

---

# STEP 7: DBT Setup

```bash
pip install dbt-core dbt-snowflake
dbt --version
dbt init netflix_project
dbt debug
```

---

# STEP 8: DBT Project Structure

| Folder | Purpose |
|---------|----------|
| `models/` | SQL transformation logic |
| `staging/` | Initial data cleanup |
| `dim/` | Dimension models |
| `fact/` | Fact models |
| `marts/` | Business-ready data |

---

# STEP 9: Staging Model Example

```sql
with raw_movies as (
    select * from MOVIELENS.RAW.RAW_MOVIES
)
select
     movieId as movie_id,
     title,
     genres
from raw_movies;
```

Run:  
```bash
dbt run
```

---

# STEP 10: Incremental Model Example

```sql
{{ config(materialized='incremental', schema_change='fail') }}

with src_ratings as (
    select * from {{ ref('src_ratings') }}
)
select 
  user_id, 
  movie_id, 
  rating, 
  rating_timestamp
from src_ratings
where rating is not null
{% if is_incremental() %}
  and rating_timestamp > (select max(rating_timestamp) from {{ this }})
{% endif %}
```

Run:  
```bash
dbt run -m fct_ratings
```

---

# STEP 11: Ephemeral Model Example

```sql
{{ config(materialized='ephemeral') }}

WITH movies AS (SELECT * FROM {{ ref('movies') }}),
tags AS (SELECT * FROM {{ ref('genome_tags') }}),
scores AS (SELECT * FROM {{ ref('genome_scores') }})
SELECT
    m.movieId,
    m.title AS movie_title,
    m.genres,
    t.tag AS tag_name,
    s.relevance AS relevance_score
FROM movies m
LEFT JOIN scores s ON m.movieId = s.movieId
LEFT JOIN tags t ON t.tagId = s.tagId;
```

---

# Notes on Ephemeral Models

- Not materialized in the database  
- Exist as **CTEs** at compile time  
- Ideal for lightweight, reusable logic  

---

# STEP 12: Seeds Example

### Seed File (CSV)
```csv
movie_id,release_date
1,1995-11-14
2,1999-05-19
3,2001-07-20
```

**Run:**
```bash
dbt seed
```

---

## Model Example Using Seed

```sql
with fct_ratings as (select * from {{ ref('fct_ratings') }}),
seed_dates as (select * from {{ ref('seed_movie_release_dates') }})
select 
  f.*,
  case when d.release_date is null then 'unknown' else 'known' end as release_info_available
from fct_ratings f
left join seed_dates d on f.movie_id = d.movie_id;
```

Run:
```bash
dbt run --select mod_movie
```

---

# STEP 13: Sources in DBT

```yaml
version: 2
sources:
  - name: netflix
    schema: raw
    tables:
      - name: r_movies
        identifier: raw_movies
        description: "Raw movies data loaded into the warehouse"
```

Use in SQL:
```sql
select movieid, title, genres 
from {{ source('netflix', 'r_movies') }};
```

---

# STEP 14: Snapshots (SCD Type 2)

```sql
{% snapshot snap_tags %}
{{ config(
  target_schema='snapshots',
  unique_key='user_id, movie_id, tag',
  strategy='timestamp',
  updated_at='tag_timestamp',
  invalidate_hard_deletes=True
) }}

select 
  user_id, 
  movie_id, 
  tag, 
  CAST(tag_timestamp AS TIMESTAMP_NTZ) AS tag_timestamp
from {{ source('src', 'tags') }}

{% endsnapshot %}
```

**Notes:**  
- Tracks changes over time using SCD Type 2  
- dbt adds columns: `dbt_valid_from`, `dbt_valid_to`

---

# STEP 15: Testing & Documentation

### Generic Tests
- `unique`
- `not_null`
- `relationships`
- `accepted_values`

### Run Tests
```bash
dbt test
```

### Documentation
```bash
dbt docs generate
dbt docs serve
```

---

# STEP 16: Macros Example

```sql
{% macro no_nulls_in_columns(model) %}
select * from {{ model }}
where
{% for column in adapter.get_columns_in_relation(model) %}
    {{ column.column }} is null
    {% if not loop.last %} or {% endif %}
{% endfor %}
{% endmacro %}
```

**Notes:**  
- Macros = reusable SQL logic  
- Reduces repetition and improves consistency  

---

# STEP 17: Analysis Example

```sql
select 
  m.title, 
  avg(r.rating) as avg_rating, 
  count(r.rating) as total_ratings
from {{ ref('fct_ratings') }} r
join {{ ref('dim_movies') }} m 
  on r.movie_id = m.movie_id
group by m.title
order by avg_rating desc;
```

**Purpose:**  
Calculates **average rating** and **total number of ratings** per movie — useful for top-rated movie dashboards.

---

# End of Project

✅ This project demonstrates a **complete ELT pipeline** using **Amazon S3 + Snowflake + dbt**.  
📊 Visualize outputs in **Tableau / Power BI / Looker Studio**.  

