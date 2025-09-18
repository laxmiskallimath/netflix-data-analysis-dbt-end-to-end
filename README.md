# Netflix Data Analysis | DBT (Data Build Tool) | End-To-End Project

---

## About the Project

This project is an end-to-end data engineering pipeline built for Netflix movie data analysis. The data is first uploaded to Amazon S3 and then loaded into the Snowflake database. Using dbt (data build tool), the data is transformed by applying key dbt concepts such as snapshots, modeling, and testing. The project follows a modern ELT (Extract, Load, Transform) approach, with dbt at the core, to ensure scalable and efficient data transformation. It demonstrates how to design and build a complete data pipeline that prepares movie data for analysis and insights.

---

## STEP 1: Project Architecture

**Project Architecture:**

The project follows a simple ELT (Extract, Load, Transform) pipeline using Amazon S3, Snowflake, and dbt as the core technologies. The processed data is then connected to BI tools such as Looker Studio, Power BI, or Tableau for visualization and analysis.

### Layers:

1. **Raw Layer (Extract & Load Layer)**
   - Data is extracted from a Netflix dataset (CSV format) and uploaded into Amazon S3.
   - From S3, the raw data is automatically loaded into Snowflake in a Raw Landing Zone.
   - **Purpose:** Keep the unmodified source data safe for reference and recovery.

2. **Staging Layer**
   - Raw data from Snowflake is copied into a Staging Zone.
   - Initial cleaning and preparation happen here before transformations.
   - **Purpose:** Acts as an intermediate layer to prevent accidental changes to raw data.

3. **Transform & Development Layer**
   - dbt is used for transformations, testing, and orchestration.
   - Data modeling techniques (like snapshots, incremental models, etc.) are applied to staging data.
   - **Purpose:** Create business-ready datasets optimized for analytics and reporting.

4. **Serving Layer**
   - Final transformed data is connected to visualization tools: Looker Studio, Power BI, Tableau.
   - **Purpose:** Build dashboards and reports for stakeholders, enabling insights and decision-making.

---

## STEP 2: Prerequisites

- **SQL Basics (Mandatory):** Ability to write SELECT statements, CTEs, joins, filters, and aggregations.
- **Snowflake Basics (Good to Have):** Understanding of databases, schemas, and tables in Snowflake.
- **Data Engineering Foundations (Good to Have):** Knowledge of ELT pipelines, and raw, staging, and serving layers.

---

## STEP 3: What is DBT?

**Definition:**

dbt (data build tool) is a transformation tool that helps you write SQL queries in a modular way to convert raw data into clean, analytics-ready datasets inside a data warehouse. It focuses only on the Transform step in ELT.

**Key Points:**

- SQL-Based transformation tool
- Modular queries
- Executes transformations inside data warehouse (Snowflake, BigQuery, Redshift)
- Core Features: Snapshots, Testing, Documentation, Deployment, Version Control, Logging & Alerting

**Why dbt?**

- **Problems before dbt:** Manual SQL scripts, complex ETL tools, scattered transformations, no testing, poor dependency handling, collaboration challenges.
- **How dbt solves:** Modularity, Reusability, Built-in Testing, Documentation, Dependency Management, Version Control, Environment Management.

---

## Foundational Concepts

- **Data Warehouse:** Central repository for structured/semi-structured/unstructured data.
- **Data Lake:** Storage for raw data in native format (e.g., S3).
- **Data Lakehouse:** Combines Data Warehouse + Data Lake.
- **ETL vs ELT:** dbt enables ELT approach, loading first, transforming later.

---

## Real Project Execution

**Dataset:** [MovieLens 20M](https://grouplens.org/datasets/movielens/20m/)

- Ratings (~20M), Users (~138K), Movies (~20K), Tags (~465K)
- CSV files: `ratings.csv`, `tags.csv`, `movies.csv`, `links.csv`, `genome-scores.csv`, `genome-tags.csv`

---

### Upload to S3

- Create S3 bucket, upload CSVs
- Example path: `s3://movielens-20m-laxmi-2025/raw-data/ratings.csv`

---

### Snowflake Setup

```sql
USE ROLE ACCOUNTADMIN;
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH;
CREATE DATABASE IF NOT EXISTS MOVIELENS;
CREATE SCHEMA IF NOT EXISTS MOVIELENS.RAW;

CREATE ROLE IF NOT EXISTS TRANSFORM;
GRANT OPERATE ON WAREHOUSE COMPUTE_WH TO ROLE TRANSFORM;
CREATE USER IF NOT EXISTS dbt PASSWORD='dbtPassword123' DEFAULT_ROLE=TRANSFORM DEFAULT_WAREHOUSE='COMPUTE_WH';
GRANT ROLE TRANSFORM TO USER dbt;

CREATE OR REPLACE STAGE NETFLIXSTAGEE
  URL='s3://dbtnetflixdataset'
  CREDENTIALS=(AWS_KEY_ID='youur key' AWS_SECRET_KEY='your secrete key');

CREATE OR REPLACE TABLE raw_movies (movieId INTEGER, title STRING, genres STRING);
COPY INTO raw_movies FROM '@NETFLIXSTAGE/movies.csv' FILE_FORMAT=(TYPE='CSV' SKIP_HEADER=1 FIELD_OPTIONALLY_ENCLOSED_BY='"');

-- Repeat for raw_ratings, raw_tags, raw_genome_scores, raw_genome_tags, raw_links
Getting Started with DBT Core

Install dbt-core and dbt-snowflake:
pip install dbt-core dbt-snowflake
dbt --version
dbt init netflix_project
Enter Snowflake credentials and verify with dbt debug

DBT Project Structure

Models: SQL statements for transformations

Materializations: view, table, incremental, ephemeral, materialized view

Folders: staging/, dim/, fact/, marts/

Example: Staging Model
with raw_movies as (
    select * from MOVIELENS.RAW.RAW_MOVIES
)
select
     movieId as movie_id,
     title,
     genres
from raw_movies


Execute: dbt run

Creates a view in Snowflake under RAW schema

Dimension Table Example
with src_movies as (
    select * from {{ ref('src_movies') }}
)
select
    movieId,
    initcap(trim(title)) as movie_title,
    split(genres, '|') as genre_array
from src_movies


Materialization configured as table in dbt_project.yml

Incremental Model Example
{{ config(materialized='incremental', schema_change='fail') }}

with src_ratings as (
    select * from {{ ref('src_ratings') }}
)
select user_id, movie_id, rating, rating_timestamp
from src_ratings
where rating is not null
{% if is_incremental() %}
  and rating_timestamp > (select max(rating_timestamp) from {{ this }})
{% endif %}


Run: dbt run -m fct_ratings

Ephemeral Model Example
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

Seeds Example

CSV: seed_movie_release_dates.csv

movie_id,release_date
1,1995-11-14
2,1999-05-19
3,2001-07-20


Use in model:

with fct_ratings as (select * from {{ ref('fct_ratings') }}),
seed_dates as (select * from {{ ref('seed_movie_release_dates') }})
select f.*,
       case when d.release_date is null then 'unknown' else 'known' end as release_info_available
from fct_ratings f
left join seed_dates d on f.movie_id = d.movie_id


Run: dbt run --select mod_movie

Sources in DBT
version: 2
sources:
  - name: netflix
    schema: raw
    tables:
      - name: r_movies
        identifier: raw_movies
        description: "Raw movies data loaded into the warehouse"


Reference using source()

select movieid, title, genres from {{ source('netflix_project', 'r_movies') }}

Snapshots (SCD Type 2)
{% snapshot snap_tags %}
{{ config(target_schema='snapshots', unique_key='user_id, movie_id, tag', strategy='timestamp', updated_at='tag_timestamp', invalidate_hard_deletes=True) }}
select user_id, movie_id, tag, CAST(tag_timestamp AS TIMESTAMP_NTZ) AS tag_timestamp
from {{ source('src', 'tags') }}
{% endsnapshot %}


DBT tracks dbt_valid_from and dbt_valid_to

Testing & Documentation

Generic Tests: unique, not_null, relationships, accepted_values

Singular Tests: Custom SQL checks

Run tests: dbt test

Generate docs: dbt docs generate + dbt docs serve

DBT Macros
{% macro no_nulls_in_columns(model) %}
select * from {{ model }}
where
{% for column in adapter.get_columns_in_relation(model) %}
    {{ column.column }} is null
    {% if not loop.last %} or {% endif %}
{% endfor %}
{% endmacro %}


Reusable SQL logic across models/tests

Analysis

Stored in analysis/ folder

Ad-hoc or business SQL queries

Example: Average ratings by movie

select m.title, avg(r.rating) as avg_rating, count(r.rating) as total_ratings
from {{ ref('fct_ratings') }} r
join {{ ref('dim_movies') }} m on r.movie_id = m.movie_id
group by m.title
order by avg_rating desc;












