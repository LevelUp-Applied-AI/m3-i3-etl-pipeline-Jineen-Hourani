[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/Nvxy3054)
# ETL Pipeline — Amman Digital Market

## Overview

This pipeline automates the extraction, transformation, and loading (ETL) process for the Amman Digital Market. It processes raw e-commerce data (customers, products, orders, and items) to generate a high-level customer analytics summary. 

The pipeline performs the following:
* **Extracts** data from a PostgreSQL database using SQLAlchemy.
* **Transforms** data using Pandas by joining tables, calculating revenue, and filtering out invalid records.
* **Validates** data integrity through automated quality checks.
* **Loads** the final analytics into a new PostgreSQL table and a CSV file for reporting.

## Setup

1. Start PostgreSQL container:
   ```bash
   docker run -d --name postgres-m3-int \
     -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres \
     -e POSTGRES_DB=amman_market \
     -p 5432:5432 -v pgdata_m3_int:/var/lib/postgresql/data \
     postgres:15-alpine
````

2.  Load schema and data (Using Docker Exec if psql is not installed locally):
    ```bash
    docker exec -i postgres-m3-int psql -U postgres -d amman_market < schema.sql
    docker exec -i postgres-m3-int psql -U postgres -d amman_market < seed_data.sql
    ```
3.  Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```

## How to Run

```bash
python etl_pipeline.py
```

## Output

The pipeline generates a `customer_analytics.csv` file in the `output/` directory and a table named `customer_analytics` in the database. The output includes:

  * **customer\_id & customer\_name**: Primary identifiers.
  * **city**: Customer location.
  * **total\_orders**: Count of non-cancelled orders.
  * **total\_revenue**: Sum of all valid order item totals.
  * **avg\_order\_value**: Average revenue generated per order.
  * **top\_category**: The product category the customer spends the most on.

## Quality Checks

The pipeline includes a mandatory validation stage to ensure data reliability:

  * **No Null IDs/Names**: Ensures every record is associated with a valid customer.
  * **Positive Revenue**: Confirms that all processed customers have a total revenue greater than zero.
  * **Unique Customer IDs**: Prevents duplicate records in the final analytics table.
  * **Orders \> 0**: Verifies that only active customers with successful orders are included.

-----

## License

This repository is provided for educational use only. See [LICENSE](https://www.google.com/search?q=LICENSE) for terms.

You may clone and modify this repository for personal learning and practice, and reference code you wrote here in your professional portfolio. Redistribution outside this course is not permitted.

```

---


