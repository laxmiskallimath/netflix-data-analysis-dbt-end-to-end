Netflix Data Analysis | DBT (databuildtool)| End-To-End Project --********************************************************************

About the project : --******************************************************************** This project is an end-to-end data engineering pipeline built for Netflix movie data analysis. The data is first uploaded to Amazon S3 and then loaded into the Snowflake database. Using dbt (data build tool), the data is transformed by applying key dbt concepts such as snapshots, modeling, and testing. The project follows a modern ELT (Extract, Load, Transform) approach, with dbt at the core, to ensure scalable and efficient data transformation. It demonstrates how to design and build a complete data pipeline that prepares movie data for analysis and insights.

STEP1 : PROJECT ARCHITECTURE (Put Image in git):

Project Architecture : --********************************************************************

The project follows a simple ELT (Extract, Load, Transform) pipeline using Amazon S3, Snowflake, and dbt (data build tool) as the core technologies. The processed data is then connected to BI tools such as Looker Studio, Power BI, or Tableau for visualization and analysis.

Raw Layer (Extract & Load Layer)
Data is extracted from a Netflix dataset (CSV format).

The dataset is uploaded into Amazon S3, which serves as the data lake.

From S3, the raw data is automatically loaded into Snowflake in a Raw Landing Zone.

Purpose: Keep the unmodified source data safe for reference and recovery.

Staging Layer
Raw data from Snowflake is copied into a Staging Zone.

Staging ensures that raw data remains untouched and acts as a safeguard.

In this layer, initial cleaning and preparation happen before transformations.

Purpose: Acts as an intermediate layer to prevent accidental changes to raw data.

Transform & Development Layer
dbt is used for transformations, testing, and orchestration.

Data modeling techniques (like snapshots, incremental models, etc.) are applied to staging data.

The transformed data is stored in the Development Zone (within Snowflake).

Purpose: Create business-ready datasets optimized for analytics and reporting.

Serving Layer
Final transformed data is connected to visualization tools.

Looker Studio

Power BI

Tableau

Purpose: Build dashboards and reports for stakeholders, enabling insights and decision-making.

STEP 2: PREREQUISITES --********************************************************************

To work with dbt (data build tool), you should have:

SQL Basics (Mandatory) : Ability to write simple queries, SELECT statements, and CTEs (Common Table Expressions.Knowledge of basic SQL operations like filtering, joins, and aggregations.

Snowflake Basics (Good to Have) : Understanding of databases, schemas, and tables in Snowflake.Helps in setting up and managing data pipelines.

Data Engineering Foundations (Good to Have) : Basic knowledge of data pipelines (Extract, Load, Transform).Familiarity with raw, staging, and serving layers in a warehouse.

STEP 3 : WHAT IS DBT? PUT DEFINATION --********************************************************************

What is dbt?
dbt (data build tool) is a transformation tool that helps you write SQL queries in a modular way to convert raw data into clean, analytics-ready datasets inside a data warehouse.

It doesn’t extract or load data — it only focuses on the transformation step of the ELT process.

Key Points about dbt:
Transformation Tool :dbt applies business logic to raw data (cleaning, renaming, aggregating, creating new columns, removing nulls). It makes the data ready for analytics and reporting.

SQL-Based : dbt is completely based on SQL. You can use SQL directly to build transformations.

Modular Queries: SQL queries can be divided into smaller chunks (models). Makes transformations easier to maintain and reuse.

SQL Data Pipeline: dbt acts like a SQL pipeline tool, sitting on top of data warehouses like Snowflake, BigQuery, or Redshift. It connects directly to the warehouse and executes transformations there.

Part of ELT, Not ETL : dbt works after data is extracted and loaded into the warehouse. Focuses only on the T (Transform) in ELT.

Core Features: Snapshots → Capture changes in data over time. Testing → Validate data quality with tests. Documentation → Auto-generate documentation from code. Deployment → Schedule and run transformation pipelines. Version Control → Manage code using Git. Logging & Alerting → Monitor pipeline runs.

Output : Final transformed datasets can be used in BI tools (Power BI, Looker Studio, Tableau) or for ML models.

Why dbt?
Why dbt? 🚨 Problems before dbt Manual SQL scripts → Hard to manage, no version control. Traditional ETL tools → Complex, expensive, required specialized skills (Spark, Scala, Java, etc.). Scattered transformations → Spread across multiple languages, making them messy and hard to maintain. No testing/documentation → Data quality issues and lack of clarity. Poor dependency handling → Execution order had to be managed manually. Collaboration challenges → Large teams struggled to work together efficiently.

✅ How dbt solves these problems Modularity → Breaks large SQL scripts into smaller, reusable models. Reusability → Common SQL logic can be reused across projects. Built-in Testing → Ensures data quality (e.g., uniqueness, no nulls, correct data types). Automatic Documentation → Generates clear, visual documentation for pipelines.

Dependency Management → Runs transformations in the correct order automatically. Version Control & Collaboration → Works smoothly with Git. Environment Management → Separate dev, test, and prod workflows. 🚀 Why it matters now Modern cloud data warehouses (Snowflake, BigQuery, Redshift) can handle raw/semi-structured data directly. Shift from ETL (Extract → Transform → Load) to ELT (Extract → Load → Transform). dbt makes transformation simpler, faster, and cheaper, leveraging SQL (which most analysts already know).

Foundational Concepts for dbt
Data Warehouse
A central repository for storing and analyzing structured data.

Earlier → Only handled structured data.

Modern warehouses (Snowflake, BigQuery, Redshift) → Support structured, semi-structured, and unstructured data.

Benefit → You can dump raw data directly and transform later using dbt (ELT approach).

Data Lake
A storage system that holds large volumes of raw data in its native format.

Example: Amazon S3.

Useful for ad-hoc analysis → You can query only the data/columns you need (e.g., with Amazon Athena).

Advantage → No need to transform before storing.

Data Lakehouse
A combination of Data Warehouse + Data Lake.

Supports both:

Structured, curated data (warehouse use cases).

Raw/unstructured data access (lake use cases).

Provides flexibility + scalability.

ETL vs ELT
ETL (Extract → Transform → Load)

Data is transformed before loading into the warehouse.

Example: Traditional systems.

ELT (Extract → Load → Transform)

Data is loaded first into modern warehouses.

Transformations happen inside the warehouse using dbt.

Benefit → Faster, cheaper, and leverages warehouse power.

Real Project Execution
Data Set / Data Source link : https://grouplens.org/datasets/movielens/20m/

📊 About the MovieLens 20M Dataset :

Size: ~20 million ratings.

Users: ~138,000.

Movies: ~20,000.

Tags: ~465,000 user-applied tags.

Files inside the dataset:

ratings.csv →

Columns: userId, movieId, rating, timestamp

Ratings range: 0.5 – 5.0

tags.csv →

Columns: userId, movieId, tag, timestamp

User-generated short text tags (metadata/feedback).

movies.csv →

Columns: movieId, title, genres

Genres are pipe (|) separated (e.g., Action|Adventure|Drama).

links.csv →

Columns: movieId, imdbId, tmdbId

Links movies to IMDb and TMDB datasets.

genome-scores.csv →

Columns: movieId, tagId, relevance

Shows how relevant a tag is to a movie.

genome-tags.csv →

Columns: tagId, tag

Lookup table for genome tags.
Download: MovieLens 20M dataset

Extract the .zip file to get CSVs.

Store them in a folder (for loading to Snowflake).

Load Data into Cloud Storage
Upload CSVs into Amazon S3 bucket (or GCP bucket if you prefer). Example path: s3://movielens-20m/ratings.csv

Steps to create S3 bucket & upload files:
Login to AWS console → search for S3.

Click Create bucket.

Give a unique name.

Pick a region .

Keep Block Public Access ON.

Click Create bucket.

Open your new bucket.

Click Upload → select your CSV files (ratings.csv, tags.csv, movies.csv etc).

Click Upload again.

Files are now in S3. Path looks like:

s3://movielens-20m-laxmi-2025/raw-data/ratings.csv

Snowflake Setup for DBT Project (MovieLens - Netflix Data)
-- Snowflake user creation

-- Step 1: Use an admin role USE ROLE ACCOUNTADMIN;

-- Step 2: Create a default warehouse CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH;

-- Step 3: Create a database and schema for the MovieLens project CREATE DATABASE IF NOT EXISTS MOVIELENS; CREATE SCHEMA IF NOT EXISTS MOVIELENS.RAW;

-- Set defaults USE WAREHOUSE COMPUTE_WH; USE DATABASE MOVIELENS; USE SCHEMA RAW;

-- Step 4: Create the transform role and assign it to ACCOUNTADMIN CREATE ROLE IF NOT EXISTS TRANSFORM; GRANT OPERATE ON WAREHOUSE COMPUTE_WH TO ROLE TRANSFORM; GRANT ROLE TRANSFORM TO ROLE ACCOUNTADMIN;

-- Step 5: Create the dbt user and assign to the transform role CREATE USER IF NOT EXISTS dbt PASSWORD='dbtPassword123' LOGIN_NAME='dbt' MUST_CHANGE_PASSWORD=FALSE DEFAULT_WAREHOUSE='COMPUTE_WH' DEFAULT_ROLE=TRANSFORM DEFAULT_NAMESPACE='MOVIELENS.RAW' COMMENT='DBT user used for data transformation'; ALTER USER dbt SET TYPE = LEGACY_SERVICE; GRANT ROLE TRANSFORM TO USER dbt;

-- Step 6: Grant permissions to the transform role GRANT ALL ON WAREHOUSE COMPUTE_WH TO ROLE TRANSFORM; GRANT ALL ON DATABASE MOVIELENS TO ROLE TRANSFORM; GRANT ALL ON ALL SCHEMAS IN DATABASE MOVIELENS TO ROLE TRANSFORM; GRANT ALL ON FUTURE SCHEMAS IN DATABASE MOVIELENS TO ROLE TRANSFORM; GRANT ALL ON ALL TABLES IN SCHEMA MOVIELENS.RAW TO ROLE TRANSFORM; GRANT ALL ON FUTURE TABLES IN SCHEMA MOVIELENS.RAW TO ROLE TRANSFORM;

--- Next step is We need to create a stage in Snowflake so that it can directly connect to our AWS S3 bucket. --- While creating the stage, we must provide AWS S3 credentials (Access Key & Secret Key) so Snowflake can read data from the bucket.

Steps to Create AWS S3 Stage in Snowflake
Go to AWS → IAM (Identity and Access Management) : This is the service to manage users, roles, and permissions.

Create a new IAM user : Example name: snowflake_netflix_user and Choose programmatic access only (no console access).

Attach a policy : Attach AmazonS3FullAccess policy (or a specific bucket access policy)

Generate Access Keys: After creating the user, go to Security credentials → Access keys.

Click Create access key → Choose Local code as use case.

Acknowledge and create → Download the CSV file (contains AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY).

Keep it safe, this file can only be downloaded once.

Go to Snowflake worksheet and create a stage using these credentials:----
CREATE OR REPLACE STAGE NETFLIXSTAGEE URL='s3://dbtnetflixdataset' CREDENTIALS=(AWS_KEY_ID='your access key' AWS_SECRET_KEY='your secrete key');

---- Data loading into tables using copy command

-- Load raw_movies CREATE OR REPLACE TABLE raw_movies ( movieId INTEGER, title STRING, genres STRING );

COPY INTO raw_movies FROM '@NETFLIXSTAGE/movies.csv' FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

-- Load raw_ratings CREATE OR REPLACE TABLE raw_ratings ( userId INTEGER, movieId INTEGER, rating FLOAT, timestamp BIGINT );

COPY INTO raw_ratings FROM '@NETFLIXSTAGE/ratings.csv' FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

-- Load raw_tags CREATE OR REPLACE TABLE raw_tags ( userId INTEGER, movieId INTEGER, tag STRING, timestamp BIGINT );

COPY INTO raw_tags FROM '@NETFLIXSTAGE/tags.csv' FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"') ON_ERROR = 'CONTINUE';

-- Load raw_genome_scores CREATE OR REPLACE TABLE raw_genome_scores ( movieId INTEGER, tagId INTEGER, relevance FLOAT );

COPY INTO raw_genome_scores FROM '@NETFLIXSTAGE/genome-scores.csv' FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

-- Load raw_genome_tags CREATE OR REPLACE TABLE raw_genome_tags ( tagId INTEGER, tag STRING );

COPY INTO raw_genome_tags FROM '@NETFLIXSTAGE/genome-tags.csv' FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

-- Load raw_links CREATE OR REPLACE TABLE raw_links ( movieId INTEGER, imdbId INTEGER, tmdbId INTEGER );

COPY INTO raw_links FROM '@NETFLIXSTAGE/links.csv' FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

Getting Started with DBT Core
Installation and Setup :

There are two main ways to use DBT, but here we are proceeding with DBT Core inside PyCharm IDE. DBT Cloud: A web-based service that provides a development environment, job scheduler, and documentation hosting. DBT Core: The open-source command-line version that you can run locally or on your servers.

Steps to Start DBT Core in PyCharm IDE (with Virtual Env):

Open Terminal in PyCharm Go to View → Tool Windows → Terminal (or press Alt + F12). Make sure your terminal prompt shows your virtual environment name (e.g., (venv)).
Step 2: Install DBT Core pip install dbt-core

Step 3: Install DBT Snowflake Adapter pip install dbt-snowflake

Step 4: Verify Installation Check the installed version with:

dbt --version

dbt init netflix_project
dbt Project Initialization Steps
Create Your First DBT Project

My folder name in pycharm = DBT_NETFLIX

My dbt project name will be whatever I entered when I ran:

Select database type:

1 (Snowflake)

Select authentication method:

1 (Password)

Enter Snowflake details when prompted:

Account → 'NWXAOGT-ZD14731'

User → dbt

Password → dbtPassword123

Role → TRANSFORM

Warehouse → COMPUTE_WH

Database → MOVIELENS

Schema → RAW

Threads → 1

Verify connection:dbt debug

Key Points About .yml Files in DBT:

Purpose:

.yml files are used to configure models, document metadata (e.g., descriptions), and define tests for data quality (e.g., not_null, unique).

Location:

dbt_project.yml: The main configuration file for your project (in the root directory).

Model .yml Files: Located in the models/ directory to configure each models metadata and tests.

Test .yml Files: Used to define custom tests for models (in the tests/ directory).

Key Configurations:

Model Descriptions: Document models and columns.

Tests: Define data integrity checks like unique, not_null.

Git:

Track .yml files with Git for version control.
DBT MODELS
DBT models are SQL statements that transform your data. Models are the core building blocks of DBT projects and represent the transformations applied to your data.

File Structure: Models are defined within .sql files located in the models folder of the DBT project. Models can be organized into subfolders such as staging, intermediate, and final to structure the transformation pipeline.

Model Creation: A model contains a single SELECT statement. We can create a model by writing a simple SQL query inside the .sql file. Example: Creating a staging_movies.sql file that transforms raw movie data.

Materialization: Materialization controls how DBT models are stored in the data warehouse. By default, DBT creates models as views. We can configure it to create tables, incremental tables, or use ephemeral models.

Referencing Other Models: Models can reference other models using the ref() function.This ensures the models are executed in the correct order based on their dependencies.

DBT Run:Once models are defined, We run them using the dbt run command.

This executes the SQL transformations, creating the tables or views in the data warehouse.

Error Handling: If there is an error in the SQL script, DBT will throw an error and stop the process, allowing you to debug and fix issues before retrying.

Snowflake Integration: The script connects to Snowflake, a cloud data warehouse, to execute SQL queries and manage data models.

Testing and Documentation: We can test your models and document them using the schema.yml file, ensuring data quality and clear project documentation.

Output Verification: After running the model, verify the transformations by checking the tables or views created in the data warehouse (e.g., Snowflake).

Example Code: with raw_movies as ( select * from MOVIELENS.RAW.RAW_MOVIES ) select movieId as movie_id, title, genres from raw_movies

In this project, I worked on transforming raw data using DBT (Data Build Tool) and Snowflake. The process involved organizing the raw data, creating staging models, and understanding DBTs materialization strategies to optimize queries and manage data effectively.

Step-by-Step Process:
Creating a Folder for Staging Models:

First, I created a new folder within my project structure called staging, where I would store all my intermediate transformation logic.

To keep the project organized, I named the folder to indicate that it holds the staging models.

Creating the Staging SQL File:Inside the staging folder, I created a new SQL file named src_movies.sql. This file will handle the transformation logic for the raw movie data.

I started with a basic SQL script to select all records from the raw data source (movie_lens.raw_movies) and replicate it into the staging model.

-- Staging script for raw movie data SELECT movie_id AS movie_id, title AS title, genre AS genre FROM movie_lens.raw_movies;

src_movies.sql.file contains below code

with raw_movies as ( select * from MOVIELENS.RAW.RAW_MOVIES ) select movieId as movie_id, title, genres from raw_movies

Understanding the Raw and Staging Layers:

Raw Layer: The raw data is untouched, as it comes from external sources (like the movie_lens.raw_movies table in Snowflake).

Staging Layer: I replicated the raw data into the staging layer (staging.src_movies) and optionally made changes or transformations, such as renaming columns for better clarity.

Executing DBT Models: Once the SQL script was ready, I used the DBT command dbt run to execute the transformation. This command runs all models and builds the tables or views in Snowflake.

I executed this command from within my project directory, ensuring that I was inside the correct virtual environment.

dbt run

After running the command, DBT created a view in Snowflake, which is a virtual table that doesn’t store data but always reflects the latest data from the source table.

Materializations in DBT:
By default, DBT creates views. However, DBT allows us to configure different materializations: View: A virtual table that runs a SQL query each time it’s accessed. Table: A physical table that stores the data. Incremental: Only inserts or updates new data since the last model run. Ephemeral: Temporary models used within other models but not stored in the database. Materialized View: A hybrid between views and tables, providing faster query performance while maintaining freshness.

Understanding these materializations is critical for optimizing the project’s performance and choosing the right storage strategy for each model.

Verifying the Transformation:

After executing the DBT model, I checked Snowflake to ensure the view was created successfully.

I verified that the transformed data had been loaded correctly with columns like movie_id, title, and genre.

-- Query to verify the view in Snowflake SELECT * FROM staging.src_movies;

.Inside the dbt project, I created a staging folder under models to define all staging models for the raw MovieLens tables. Each raw table (movies, ratings, tags, links, genome_scores, genome_tags) has a corresponding SQL model inside the staging folder, where I applied light transformations such as renaming columns for consistency (e.g., movieId → movie_id, tagId → tag_id) and converting timestamps into proper formats using TO_TIMESTAMP. Once the models were defined, I executed dbt run, which materialized these models as views in Snowflake under the RAW schema. A total of six staging views (src_movies, src_ratings, src_tags, src_links, src_genome_scores, src_genome_tags) were created successfully, providing a clean and standardized layer for further downstream transformations.

Creating Tables, Dimensions, and Incremental Models in Snowflake
1.Default Materialization in DBT : By default, all DBT models are materialized as views.If we want to create tables or incremental tables, we need to configure the materialization.

2.Creating Dimension and Fact Tables

Inside the models/ folder, create a new folder called dim/ (for dimension tables).

models/ ├── staging/ ├── dim/

Changing materialization :
Configure DBT so that everything inside the dim/ folder is materialized as a table:

open dbt_project.yml
models: netflix_project: +materialized: view dim : +materialized: table

-- Now, any SQL file placed inside the dim/ folder will create a table instead of a view.

3.Creating a Dimension Table (Example: dim_movies) Create a new file inside dim/: models/dim/dim_movies.sql Reference the staging model using DBT’s ref() function:

with src_movies as ( select * from {{ ref('src_movies') }} ) select movieId, initcap(trim(title)) as movie_title, split(genres, '|') as genre_array from src_movies;

-- This will create a table named dim_movies in Snowflake.

Run the model: dbt run

We should see logs like: Table model dim_movies created

Creating More Dimension Tables like dim_users, dim_genome_tags etc..... Run everything using command: dbt run
Now we have multiple dimension tables created in Snowflake.

Running Specific Models
Instead of running everything, you can run a specific model dbt run --models dim_users

Fixing Schema Conflicts (Raw vs Dev Schema) Raw data is stored in the raw schema (directly loaded from S3 or other sources). Development data should be stored in the dev schema, where we perform transformations (staging, dimension, and fact models) using DBT. This separation ensures that the raw data remains untouched and clean, while all transformations happen in the dev schema without disturbing the raw layer. By default, DBT may create both raw views and dim tables in the same schema. 👉 To separate them, update your profiles.yml to write into a dev schema.
netflix_project: outputs: dev: account: NWXAOGT-ZD14731 database: MOVIELENS password: dbtPassword123 role: TRANSFORM schema: DEV # 👈 changed from raw to dev threads: 1 type: snowflake user: dbt warehouse: COMPUTE_WH target: dev

Now, DBT will:Keep raw tables in raw schema and Create dim/fact models in dev schema

Cleaning Up Old Views/Tables in Snowflake If we need a clean project drop all projects which were created in Raw schema :
drop table MOVIELENS.RAW.dim_movies; drop table MOVIELENS.RAW.dim_users; drop table MOVIELENS.RAW.dim_genome_tags; drop view MOVIELENS.RAW.src_movies; drop view MOVIELENS.RAW.src_ratings; drop view MOVIELENS.RAW.src_tags; drop view MOVIELENS.RAW.src_genome_tags; drop view MOVIELENS.RAW.src_links;

Understand Slowly Changing Dimensions (SCD)
SCD Type 1: Overwrite old values (no history maintained).

SCD Type 2: Maintain full history by adding a new row with:

Flags (is_active = true/false), OR Version numbers, OR Start and end dates (end_date = null means active).

SCD Type 3: Maintain limited history by adding extra columns (e.g., current_city, previous_city).

SCD Type 6: Combination of Types 1, 2, and 3 (track previous values, versions, and active flags).

🔑 Challenge: Writing custom logic for SCDs in raw SQL is tedious → dbt incremental + snapshots make it easier.

Create / Implement Incremental Model for Ratings
Inside the Fact folder, create a new file: fct_ratings.sql

Add config block to set materialization as incremental:

{{ config( materialized='incremental', schema_change='fail' ) }}

materialized='incremental' → defines the model as incremental.

schema_change='fail' → prevents updates if the source schema changes.

-- Base query:

with src_ratings as ( select * from {{ ref('src_ratings') }} )

select user_id, movie_id, rating, rating_timestamp from src_ratings where rating is not null

-- Add Incremental Logic

Use dbt’s is_incremental() function:

{% if is_incremental() %} and rating_timestamp > (select max(rating_timestamp) from {{ this }}) {% endif %}

Incremental Logic Explanation: When new rows are added in src_ratings, check the latest timestamp.If the new record has rating_timestamp greater than the max timestamp in the target table (fct_ratings), insert it. This ensures only new rows are appended without reprocessing the full dataset.

-- Run Incremental Model(Execute only this Incremental model)

dbt run -m fct_ratings

Verify in Snowflake: A fact_ratings table should now exist.The table will grow automatically when new rows are added to the source.

Switching schemas, running queries, changing materialization from view → table, insert records, and demonstration incremental and ephemeral models in dbt.
-- Select latest 5 ratings from fact table fct_ratings in dev schema SELECT * FROM movielens.dev.fct_ratings ORDER BY rating_timestamp DESC LIMIT 5;

--Change a Source Model src_ratings which is view whre we cannot insert data so change it from View → Table (so we can insert data)

In our dbt model file (src_ratings.sql):

{{ config(materialized='table') }}

SELECT * FROM {{ source('raw', 'ratings') }};

Run: dbt run -m src_ratings

This will rebuild dev.src_ratings as a table (instead of a view).

-- Lets Insert New Data into src_ratings INSERT INTO dev.src_ratings (userId, movieId, rating, rating_timestamp) VALUES (87, 86, 4.00, '2015-03-31 00:00:00');

SELECT * FROM dev.src_ratings ORDER BY rating_timestamp DESC;

-- Refresh Incremental Model (fct_ratings)

Run only the incremental model:fct_ratings

dbt run -m fct_ratings

-- Lets Check new row in fact table:

SELECT * FROM dev.fct_ratings ORDER BY rating_timestamp DESC LIMIT 5;

--- Example of Ephemeral Model (dim_movies_with_tags.sql) {{ config(materialized='ephemeral') }} WITH movies AS ( SELECT * FROM {{ ref('movies') }} ), tags AS ( SELECT * FROM {{ ref('genome_tags') }} ), scores AS ( SELECT * FROM {{ ref('genome_scores') }} ) SELECT m.movieId, m.title AS movie_title, m.genres, t.tag AS tag_name, s.relevance AS relevance_score FROM movies m LEFT JOIN scores s ON m.movieId = s.movieId LEFT JOIN tags t ON t.tagId = s.tagId;

-- Use Ephemeral Model in Another Model

ep_movie_with_tags.sql

SELECT * FROM {{ ref('dim_movies_with_tags') }};

Run: dbt run -m ep_movie_with_tags

-- Check in Snowflake:

SELECT * FROM dev.ep_movie_with_tags LIMIT 10;

DBT Seeds
Inside the seeds folder, create a CSV file : seed_movie_release_dates.csv

movie_id,release_date 1,1995-11-14 2,1999-05-19 3,2001-07-20

Run:dbt seed

This creates a table in our DEV schema named seed_movie_release_dates.

-- Use the Seed in a Model -- Create Mart folder inside that create mod_movie.sql referencing seed file.

models/marts/mod_movie.sql

{{ config(materialized = 'table') }}

with fct_ratings as ( select * from {{ ref('fct_ratings') }} ), seed_dates as ( select * from {{ ref('seed_movie_release_dates') }} ) select f.*, case when d.release_date is null then 'unknown' else 'known' end as release_info_available from fct_ratings f left join seed_dates d on f.movie_id = d.movie_id

Run the Model : dbt run --select mod_movie This creates a final table combining ratings data with movie release dates from the seed.

Sources
Sources in dbt represent raw data tables already loaded into your data warehouse (before dbt transformations).

Configuring sources helps with:

✅ Clear documentation of where data comes from

✅ Built-in testing (e.g., uniqueness, not null)

✅ Freshness checks on raw data

Example definition: Inside your models create a source YAML file:sources.yml

version: 2

sources:

name: netflix # Source name schema: raw # Schema where raw tables exist tables:
name: r_movies # Alias for our use identifier: raw_movies # Actual table in the warehouse description: "Raw movies data loaded into the warehouse"
name: r_links identifier: raw_links description: "Raw links data"
name: Logical grouping for your source (e.g., project name).

schema: Schema where the raw tables live.

Tables: List of raw tables (with name and identifier).

Use Sources in Models:

Instead of directly querying raw tables, use the source() function.

Example model: models/staging/stg_movies.sql

{{ config(materialized = 'view') }}

select movieid, title, genres from {{ source('netflix_project', 'r_movies') }}

-- This pulls data from the raw raw_movies table defined in sources.yml.

-- Implementing SCD Type 2 with DBT Snapshots in Snowflake
Concept:

Snapshots in DBT = built-in way to implement Slowly Changing Dimension Type 2 (SCD2).

They record the state of a mutable table over time.

DBT automatically manages dbt_valid_from and dbt_valid_to to track history.

Creating a Snapshot:

Create a new file inside the snapshots/ folder → e.g., snap_tags.sql.

Example snapshot config:

{% snapshot snap_tags %}

{{ config( target_schema='snapshots', unique_key='user_id, movie_id, tag', strategy='timestamp', updated_at='tag_timestamp', invalidate_hard_deletes=True ) }}

SELECT user_id, movie_id, tag, CAST(tag_timestamp AS TIMESTAMP_NTZ) AS tag_timestamp FROM {{ source('src', 'tags') }}

{% endsnapshot %}

Key Config Parameters:

target_schema → schema where snapshot tables will be created. unique_key → identifies unique records (can use multiple columns). strategy → timestamp (detects changes using a timestamp column). updated_at → timestamp column to check for changes. invalidate_hard_deletes=True → track deletions as historical changes.

-- Surrogate Key Concept (Important)
Sometimes natural keys (user_id, movie_id, tag) are not reliable.They may not guarantee uniqueness.They may change over time. To solve this → we use a Surrogate Key (artificial key).

DBT provides a macro from dbt-utils to generate one:

dbt_utils.generate_surrogate_key(['user_id', 'movie_id', 'tag']) This generates a hashed key that uniquely identifies a row → even if multiple columns define uniqueness.

Example output:

user_id=18, movie_id=414, tag='Mark Waters' → surrogate key = abc123xyz...

Benefit: simplifies joins, ensures consistent row identity across snapshot history.

-- Installing dbt-utils (for surrogate keys)
Add this in packages.yml:

packages:

package: dbt-labs/dbt_utils version: 1.0.0
Install with: dbt deps

Running the Snapshot
Run the command:dbt snapshot

DBT checks for inserts/updates/deletes in the source table.

Adds new rows with updated dbt_valid_from and dbt_valid_to.

Testing snapshot in Snowflake
-- Query snapshot table in Snowflake:

SELECT * FROM snapshots.snap_tags ORDER BY user_id, dbt_valid_from DESC;

-- Update source data:

UPDATE dev.src_tags SET tag = 'Mark Waters Returns', tag_timestamp = CAST(CURRENT_TIMESTAMP AS TIMESTAMP_NTZ) WHERE user_id = 18;

-- Re-run snapshot:dbt snapshot

Verify new row added in snapshot table → old row gets dbt_valid_to, new row has dbt_valid_from and active NULL in dbt_valid_to.

Example Snapshot Output:

surrogate_key	user_id	movie_id	tag	tag_timestamp	dbt_valid_from	dbt_valid_to
abc123	18	414	Mark Waters	2009-04-24	2009-04-24 10:00:00	2025-09-16 12:00:00
def456	18	414	Mark Waters Returns	2025-09-16	2025-09-16 12:00:00	NULL
By configuring snapshots in DBT, you can automatically implement SCD Type 2 in Snowflake. Using surrogate keys ensures unique and consistent row tracking across historical records, even when natural keys are insufficient.

DBT Testing & Documentation
Importance of Documentation in dbt

Documentation helps new team members understand what each model, column, and test means.

When you generate docs (dbt docs generate + dbt docs serve), dbt creates an interactive website with:

Table & column descriptions.

Data types.

Applied tests.

Relationships between models.

Auto-generated lineage graph showing dependencies between sources, staging, dimensions, and fact models.

Benefits:- Acts as self-service data catalog.

Saves onboarding time for new engineers.

Ensures clarity and governance across the data warehouse.

dbt Testing Overview
dbt supports two types of tests:

Generic Tests: Reusable tests provided by dbt (applied via schema.yml). Common built-in generic tests: unique → ensures column values are unique (like primary keys). Example: movie_id, user_id.

not_null → ensures no null values in a column. Example: movie_title cannot be null.

relationships → checks referential integrity between two tables. Example: fct_ratings.movie_id must exist in dim_movies.movie_id.

accepted_values → restricts a column to a predefined set of values. Example: status column can only be ['active', 'inactive', 'pending'].

Singular Tests:Custom SQL queries that enforce business-specific logic. Created inside a tests/ folder. Example: Ensure relevance_score > 0 in fct_genome_scores: -- tests/relevance_score_positive.sql select * from {{ ref('fct_genome_scores') }} where relevance_score <= 0

dbt runs this test with: dbt test --select relevance_score_positive

If rows are returned → test fails. If no rows → test passes.

Configuring Tests in schema.yml:
A schema.yml file documents models, columns, and applies tests. Example:

version: 2

models:

name: dim_movies description: "Dimension table for cleansed movie metadata" columns:
name: movie_id description: "Primary key of the movie" tests:

unique
not_null
name: movie_title description: "Standardized movie title" tests:

not_null
name: genre_array description: "List of genres in array format"

name: genres description: "Raw genre string from sources"

Running Tests
Run all tests:dbt test Run tests for a specific model: dbt test --select dim_movies Run a specific test type:dbt test --select test_type:unique

Test Severity Levels: Tests can be configured to raise: Error → stops pipeline if test fails. Warning → logs a warning but continues pipeline.

Example: tests:

accepted_values: values: ['active', 'inactive', 'pending'] severity: warn
dbt Documentation Website
Generate documentation: dbt docs generate

Serve locally:dbt docs serve

Features:Model descriptions. Column metadata. Applied tests. Lineage graph (shows dependencies end-to-end). Sample SQL for models.

Example: fct_ratings → depends on → dim_users + dim_movies.

Practical Example of Lineage Graph
Sources (e.g., r_movies, r_ratings) →

Staging models (src_movies, src_ratings) →

Dimensions (dim_movies, dim_users) →

Facts (fct_ratings, fct_genome_scores).

This visual graph helps analysts quickly see how data flows across layers.

DBT Macros, Analysis
Macros (Functions in DBT)

Macros = reusable SQL functions in DBT.

Purpose → avoid repeating logic across multiple models/tests.

Defined inside the macros/ folder.

Syntax:

{% macro macro_name(arg1, arg2) %} -- SQL logic using arguments {% endmacro %}

✅ Example: No Nulls Check Macro

{% macro no_nulls_in_columns(model) %} select * from {{ model }} where {% for column in adapter.get_columns_in_relation(model) %} {{ column.column }} is null {% if not loop.last %} or {% endif %} {% endfor %} {% endmacro %}

Usage in tests:
select * from {{ no_nulls_in_columns(ref('fact_relevance_score')) }}

Benefit → Write once, reuse everywhere. (Macros like functions in Python/SQL.)

Analysis Files
Stored inside analysis/ folder. Contain ad-hoc or final business SQL queries (exploratory analysis, reports). They do not create warehouse objects by default. Use dbt compile to render them into SQL → can copy & run in Snowflake.

Example: analysis/movie_analysis.sql

select m.title, avg(r.rating) as avg_rating, count(r.rating) as total_ratings from {{ ref('fact_ratings') }} r join {{ ref('dim_movies') }} m on r.movie_id = m.movie_id group by m.title order by avg_rating desc;

Run:dbt compile

Output available in:

target/compiled/<project_name>/analysis/movie_analysis.sql

we can also create a final table in Snowflake:

create or replace table movielens_dev.movie_analysis as select * from {{ ref('movie_analysis') }};
