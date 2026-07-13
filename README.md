# Netflix Project — dbt + Snowflake Learning Project

A hands-on dbt learning project built on the [MovieLens](https://grouplens.org/datasets/movielens/) dataset, with Snowflake as the data warehouse. The project models movie ratings, tags, and genome scores into a clean analytics layer using dbt best practices.

## Architecture

```
Snowflake (MOVIELENS database)
│
├── RAW schema          ← raw source tables (loaded externally)
├── STAGING schema      ← thin views over raw tables (nfx_tfx staging layer)
├── DEV schema          ← dimension and fact tables (nfx_tfx dim + fct layers)
├── LOOKUP schema       ← seed data (movie_release_date)
└── SNAPSHOTS schema    ← SCD Type 2 customer snapshot
```

## Project Structure

```
netflix_project/
├── nfx_tfx/                        # dbt project root
│   ├── models/
│   │   ├── staging/                # Source-aligned views / tables
│   │   │   ├── source.yml
│   │   │   ├── src_movies.sql
│   │   │   ├── src_ratings.sql     (table)
│   │   │   ├── src_tags.sql        (table)
│   │   │   ├── src_links.sql
│   │   │   ├── src_genome_tags.sql
│   │   │   └── src_genome_scores.sql
│   │   ├── dim/                    # Dimension tables
│   │   │   ├── dim_movies.sql
│   │   │   ├── dim_genome_tags.sql
│   │   │   ├── dim_users.sql
│   │   │   └── dim_genome_movie_tag.sql  (ephemeral)
│   │   └── fct/                    # Fact tables
│   │       ├── fct_genome_scores.sql
│   │       ├── fct_movie_ratings_incr.sql  (incremental)
│   │       └── fct_movie_tag_genome.sql
│   ├── seeds/
│   │   └── movie_release_date.csv  # 10-row lookup: movie_id → release_date
│   ├── snapshots/
│   │   ├── customer_snapshot.sql   # SCD Type 2 on customers_scd (simple)
│   │   └── tag_snapshot.sql        # SCD Type 2 on src_tags (composite key + hard-delete)
│   ├── macros/                     # (empty)
│   ├── tests/                      # (empty)
│   ├── packages.yml                # External package declarations
│   ├── package-lock.yml
│   ├── dbt_project.yml
│   └── requirements.txt
└── .venv/                          # Python virtual environment
```

## Data Sources

| Source | Database | Schema | Tables |
|---|---|---|---|
| `raw` | MOVIELENS | RAW | `raw_movies`, `raw_ratings` |
| `analytics` | ANALYTICS | RAW | `customers_scd` |

> **Note:** `raw_tags`, `raw_links`, `raw_genome_tags`, and `raw_genome_scores` are not yet registered in `source.yml` — their staging models reference them via hard-coded three-part names (`MOVIELENS.RAW.*`).

## Packages

Declared in `packages.yml`, installed via `dbt deps`:

| Package | Version |
|---|---|
| `dbt-labs/dbt_utils` | 1.4.1 |

`dbt_utils` is used in `tag_snapshot.sql` to generate a surrogate key via `{{ dbt_utils.generate_surrogate_key([...]) }}`.

## Models

### Staging Layer (`STAGING` schema)

Thin wrappers that rename columns to snake_case and cast timestamps. Two models override the default view materialization to **table**.

| Model | Materialization | Description |
|---|---|---|
| `src_movies` | View | Movie titles and pipe-delimited genres |
| `src_ratings` | **Table** | User ratings with unix timestamp conversion |
| `src_tags` | **Table** | User-applied tags with timestamp conversion |
| `src_links` | View | IMDB and TMDB ID mappings per movie |
| `src_genome_tags` | View | Tag vocabulary from the genome dataset |
| `src_genome_scores` | View | Movie-tag relevance scores |

### Dimension Layer (`DEV` schema — tables)

| Model | Description |
|---|---|
| `dim_movies` | Cleaned titles (`INITCAP`), genres as an array |
| `dim_genome_tags` | Cleaned tag labels (`INITCAP`) |
| `dim_users` | Distinct user list unioned from ratings and tags |
| `dim_genome_movie_tag` | **Ephemeral** — denormalized movie + tag + relevance join |

### Fact Layer (`DEV` schema — tables)

| Model | Materialization | Description |
|---|---|---|
| `fct_genome_scores` | Table | Movie-tag relevance scores (`relevance > 0`, rounded to 4dp) |
| `fct_movie_ratings_incr` | **Incremental** | User ratings, deduped on `(user_id, movie_id)`, loads only new rows on incremental runs |
| `fct_movie_tag_genome` | Table | Selects from the ephemeral `dim_genome_movie_tag` |

### Seed

| Seed | Schema | Description |
|---|---|---|
| `movie_release_date` | LOOKUP | Maps 10 `movie_id` values (101–110) to release datetimes |

### Snapshot

| Snapshot | Strategy | Unique Key | Description |
|---|---|---|---|
| `customer_snapshot` | timestamp (`updated_at`) | `customer_id` | SCD Type 2 snapshot of `ANALYTICS.RAW.customers_scd`. Full row history written to `SNAPSHOTS` schema. |
| `tag_snapshot` | timestamp (`tag_timestamp`) | `[user_id, movie_id, tag]` | SCD Type 2 snapshot of `src_tags` (LIMIT 100). Composite unique key with a surrogate key generated via `dbt_utils.generate_surrogate_key`. Hard-delete invalidation enabled (`invalidate_hard_deletes = True`). |

## Snowflake Database Setup

Run the following steps in Snowflake (as `ACCOUNTADMIN`) to create the warehouse, user, database, and raw tables before running dbt.

### Step 1 — Create role, warehouse, and dbt user

```sql
USE ROLE ACCOUNTADMIN;

CREATE ROLE IF NOT EXISTS TRANSFORM;
GRANT ROLE TRANSFORM TO ROLE ACCOUNTADMIN;

CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH;
GRANT OPERATE ON WAREHOUSE COMPUTE_WH TO ROLE TRANSFORM;

CREATE USER IF NOT EXISTS dbt
  PASSWORD='dbtPassword123'
  LOGIN_NAME='dbt'
  MUST_CHANGE_PASSWORD=FALSE
  DEFAULT_WAREHOUSE='COMPUTE_WH'
  DEFAULT_ROLE=TRANSFORM
  DEFAULT_NAMESPACE='MOVIELENS.RAW'
  COMMENT='DBT user used for data transformation';
ALTER USER dbt SET TYPE = LEGACY_SERVICE;
GRANT ROLE TRANSFORM TO USER dbt;
```

### Step 2 — Create database, schema, and grant permissions

```sql
CREATE DATABASE IF NOT EXISTS MOVIELENS;
CREATE SCHEMA IF NOT EXISTS MOVIELENS.RAW;

GRANT ALL ON WAREHOUSE COMPUTE_WH TO ROLE TRANSFORM;
GRANT ALL ON DATABASE MOVIELENS TO ROLE TRANSFORM;
GRANT ALL ON ALL SCHEMAS IN DATABASE MOVIELENS TO ROLE TRANSFORM;
GRANT ALL ON FUTURE SCHEMAS IN DATABASE MOVIELENS TO ROLE TRANSFORM;
GRANT ALL ON ALL TABLES IN SCHEMA MOVIELENS.RAW TO ROLE TRANSFORM;
GRANT ALL ON FUTURE TABLES IN SCHEMA MOVIELENS.RAW TO ROLE TRANSFORM;

-- If using the ANALYTICS database for the customer snapshot
GRANT ALL ON DATABASE ANALYTICS TO ROLE TRANSFORM;
GRANT ALL ON ALL SCHEMAS IN DATABASE ANALYTICS TO ROLE TRANSFORM;
GRANT ALL ON FUTURE SCHEMAS IN DATABASE ANALYTICS TO ROLE TRANSFORM;
GRANT ALL ON ALL TABLES IN SCHEMA ANALYTICS.RAW TO ROLE TRANSFORM;
GRANT ALL ON FUTURE TABLES IN SCHEMA ANALYTICS.RAW TO ROLE TRANSFORM;
```

### Step 3 — Create an S3 stage and raw tables

The raw CSV files are stored in an S3 bucket. Create a named stage pointing to it, then create and populate each raw table.

```sql
USE SCHEMA MOVIELENS.RAW;

-- Create external stage (replace credentials with your own)
CREATE STAGE netflixstage
  URL='s3://netflix-bucket-rt'
  CREDENTIALS=(AWS_KEY_ID='<your-key-id>' AWS_SECRET_KEY='<your-secret-key>');

-- Movies
CREATE OR REPLACE TABLE raw_movies (
  movies_id INTEGER,
  title     STRING,
  genres    STRING
);
COPY INTO raw_movies
FROM '@netflixstage/movies.csv'
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

-- Ratings
CREATE OR REPLACE TABLE raw_ratings (
  userId    INTEGER,
  movieId   INTEGER,
  rating    FLOAT,
  timestamp BIGINT
);
COPY INTO raw_ratings
FROM '@netflixstage/ratings.csv'
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

-- Genome scores
CREATE OR REPLACE TABLE raw_genome_scores (
  movieId   INTEGER,
  tagId     INTEGER,
  relevance FLOAT
);
COPY INTO raw_genome_scores
FROM '@netflixstage/genome-scores.csv'
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

-- Tags
CREATE OR REPLACE TABLE raw_tags (
  userId    INTEGER,
  movieId   INTEGER,
  tag       STRING,
  timestamp BIGINT
);
COPY INTO raw_tags
FROM '@netflixstage/tags.csv'
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

-- Genome tags
CREATE OR REPLACE TABLE raw_genome_tags (
  tagId INTEGER,
  tag   STRING
);
COPY INTO raw_genome_tags
FROM '@netflixstage/genome-tags.csv'
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

-- Links
CREATE OR REPLACE TABLE raw_links (
  movieId INTEGER,
  imdbId  INTEGER,
  tmdbId  INTEGER
);
COPY INTO raw_links
FROM '@netflixstage/links.csv'
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');
```

After completing these steps, the `MOVIELENS.RAW` schema will have all six raw tables populated and ready for dbt to consume.

---

## Setup

### Prerequisites

- Python 3.8+
- A Snowflake account with the `MOVIELENS` database and raw tables loaded
- dbt profile configured at `~/.dbt/profiles.yml`

### Install dependencies

```bash
cd netflix_project
python -m venv .venv
.venv/Scripts/activate      # Windows
pip install -r nfx_tfx/requirements.txt
```

### Initialise a new dbt project (optional — skip if using this repo as-is)

`dbt init` scaffolds a new project interactively. Run it from the directory where you want the project folder created:

```bash
dbt init nfx_tfx
```

You will be prompted for:
1. The database adapter (choose `snowflake`)
2. Connection details (account, user, password, role, warehouse, database, schema, threads)

This creates the `nfx_tfx/` folder with the standard dbt directory structure and writes a starter profile to `~/.dbt/profiles.yml`. If the project already exists (as in this repo), skip this step and configure the profile manually instead.

### Configure the dbt profile

Add the following to `~/.dbt/profiles.yml`:

```yaml
nfx_tfx:
  outputs:
    dev:
      type: snowflake
      account: <your-account>
      database: MOVIELENS
      schema: NFX
      warehouse: COMPUTE_WH
      role: TRANSFORM
      user: <your-user>
      password: <your-password>
      threads: 1
  target: dev
```

### Run the project

```bash
cd nfx_tfx

# Install packages
dbt deps

# Load seed data
dbt seed

# Run all models
dbt run

# Run snapshots
dbt snapshot

# Run a specific layer
dbt run --select staging
dbt run --select dim
dbt run --select fct

# Run the incremental model in full-refresh mode
dbt run --select fct_movie_ratings_incr --full-refresh
```

## Key Concepts Practiced

- **Layered modelling** — staging → dim → fct separation
- **Materializations** — views, tables, incremental, ephemeral
- **Per-model materialization overrides** — `src_ratings` and `src_tags` override the staging default to `table`
- **Incremental models** — loading only new rows using `MAX(rating_timestamp)`
- **Seeds** — loading static lookup data into Snowflake
- **Snapshots (SCD Type 2)** — simple timestamp strategy (`customer_snapshot`) and advanced composite-key snapshot with hard-delete invalidation and surrogate key generation (`tag_snapshot`)
- **External packages** — `dbt-labs/dbt_utils` for surrogate key generation via `generate_surrogate_key`
- **Source definitions** — declaring raw tables in `source.yml`
