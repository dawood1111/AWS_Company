# Organizations Data Pipeline on Databricks (AWS S3 → Delta → Dashboard)

An end-to-end data engineering project. Company CSV files land in an AWS S3 folder every day. A pipeline loads only the new files, cleans them, builds business-ready tables, and shows the results in dashboards.

## Project Goal

A company receives a new organizations file every day and wants to:
1. Load new files automatically, without reloading old ones or losing history.
2. Clean the data and apply business rules.
3. Answer questions about countries, industries, and company sizes.
4. See the results in a dashboard.

## Architecture

```
AWS S3 folder (daily CSV files)
        │
        ▼
Bronze  → streaming table (loads only new files, keeps all history)
        │
        ▼
Silver  → cleaned data (notebook)
        │
        ▼
Gold    → business tables (notebook)
        │
        ▼
Dashboards (Databricks dashboard + Power BI)

Orchestration: a daily Databricks Job with email notifications
```

## Tech Stack

- Databricks (Unity Catalog, Jobs, SQL warehouse, dashboards)
- AWS S3 (storage), IAM role, external location
- PySpark and SQL
- Delta Lake
- Power BI Desktop
- Git and GitHub

## What I Did

### 1. Connected Databricks to AWS S3
- Created an IAM role, a storage credential, and an external location for the bucket `[bucket name]`.
- Tested the connection before loading data.

### 2. Bronze layer: incremental ingestion
- Created a **streaming table** with the **Scheduled (folders only)** ingest option, which reads the S3 folder with `read_files`.
- It loads **only new files** on a schedule and appends them, so earlier days are never overwritten.
- Tested it by adding files one at a time and checking that the row count grew by only the new file's rows.

### 3. Silver layer: cleaning and business rules
Built in a PySpark notebook:
- `company_size`: Small, Medium, or Enterprise, based on number of employees.
- `company_age`: calculated with `year(current_date()) - founded`, so the year is never hardcoded.
- Website cleaned to a plain domain with `regexp_extract`.
- Industry split into `main_industry` and `sub_industry` with `split`.
- Duplicates removed by organization ID.
- Column names converted to snake_case, because Delta rejects spaces.

### 4. Gold layer: business tables
| Table | Question it answers |
|---|---|
| `aws_gold_country_avg_vs_global` | Which countries have above-average employees? (cross join) |
| `aws_gold_country_employee_...` | `[what it shows, e.g. employee share per country using a window]` |
| `aws_gold_industry_count_by_...` | How many companies per industry and size? (pivot) |
| `aws_gold_top_industries_per_...` | Top industries per country (`dense_rank` window) |

### 5. Orchestration
- Created a Databricks **Job** that runs daily: refresh the streaming table, then run the silver and gold notebook.
- Added email notifications for start, success, and failure.


### 6. Dashboards
- **Power BI:** connected to the SQL warehouse and built `[describe your visuals]`.

## Screenshots

![Pipeline and job]
<img width="1276" height="682" alt="image" src="https://github.com/user-attachments/assets/45463276-545d-4d1c-a9e4-286503f461ea" />
<img width="1701" height="927" alt="image" src="https://github.com/user-attachments/assets/71bad6e5-d9b1-475d-be6d-b8dfedc5eab6" />


![Power BI]
<img width="1856" height="1076" alt="image" src="https://github.com/user-attachments/assets/f3847fd8-de92-4e6d-bbaf-9bfd7d34bed0" />



`dawood adel jaber`
