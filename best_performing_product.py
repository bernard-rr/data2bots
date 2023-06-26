import psycopg2
import boto3
from psycopg2 import sql

# Connect to db
s3 = boto3.client('s3')
bucket_name = "d2b-internal-assessment-bucket"

# Establish a connection to the data warehouse
conn = psycopg2.connect(
    database="d2b_accessment",
    user="emmachid6410",
    password="pGeOqdZMAF",
    host="34.89.230.185",
    port=5432)
print("Database connected successfully")
cur = conn.cursor()

# Create the "best_performing_product" table if it doesn't exist
create_table_query = """
    CREATE TABLE IF NOT EXISTS emmachid6410_analytics.best_performing_product (
        order_date DATE,
        ingestion_date DATE NOT NULL DEFAULT CURRENT_DATE,
        product_name VARCHAR NOT NULL,
        most_ordered_day DATE NOT NULL,
        is_public_holiday BOOLEAN NOT NULL,
        tt_review_points INT NOT NULL,
        pct_one_star_review FLOAT NOT NULL,
        pct_two_star_review FLOAT NOT NULL,
        pct_three_star_review FLOAT NOT NULL,
        pct_four_star_review FLOAT NOT NULL,
        pct_five_star_review FLOAT NOT NULL,
        pct_early_shipments FLOAT,
        pct_late_shipments FLOAT
    )
"""
cur.execute(create_table_query)

# Truncate the agg_public_holiday table before populating new data
truncate_query = sql.SQL("TRUNCATE TABLE emmachid6410_analytics.best_performing_product")
cur.execute(truncate_query)

# Query to insert data into the table
insert_query = """
    INSERT INTO emmachid6410_analytics.best_performing_product
    SELECT
        DATE_TRUNC('month', o.order_date) AS order_date,
        CURRENT_DATE AS ingestion_date,
        p.product_name,
        MAX(o.order_date) AS most_ordered_day,
        CASE WHEN d.working_day = FALSE AND d.day_of_the_week_num BETWEEN 1 AND 5 THEN TRUE ELSE FALSE END AS is_public_holiday,
        SUM(r.review) AS tt_review_points,
        COUNT(CASE WHEN r.review = 1 THEN 1 END) * 100.0 / NULLIF(COUNT(r.review), 0) AS pct_one_star_review,
        COUNT(CASE WHEN r.review = 2 THEN 1 END) * 100.0 / NULLIF(COUNT(r.review), 0) AS pct_two_star_review,
        COUNT(CASE WHEN r.review = 3 THEN 1 END) * 100.0 / NULLIF(COUNT(r.review), 0) AS pct_three_star_review,
        COUNT(CASE WHEN r.review = 4 THEN 1 END) * 100.0 / NULLIF(COUNT(r.review), 0) AS pct_four_star_review,
        COUNT(CASE WHEN r.review = 5 THEN 1 END) * 100.0 / NULLIF(COUNT(r.review), 0) AS pct_five_star_review,
        COUNT(CASE WHEN s.shipment_date < (o.order_date + INTERVAL '6 days') AND s.delivery_date IS NOT NULL THEN 1 END) * 100.0 / NULLIF(COUNT(CASE WHEN s.delivery_date IS NOT NULL THEN 1 END), 0) AS pct_early_shipments,
        COUNT(CASE WHEN s.shipment_date >= (o.order_date + INTERVAL '6 days') AND s.delivery_date IS NULL THEN 1 END) * 100.0 / NULLIF(COUNT(CASE WHEN s.delivery_date IS NULL THEN 1 END), 0) AS pct_late_shipments
    FROM emmachid6410_staging.orders o
    JOIN emmachid6410_staging.shipments_deliveries s ON o.order_id = s.order_id
    JOIN if_common.dim_products p ON o.product_id = p.product_id
    JOIN if_common.dim_dates d ON o.order_date = d.calendar_dt
    JOIN emmachid6410_staging.reviews r ON p.product_id = r.product_id
    GROUP BY order_date, product_name, is_public_holiday
"""

# Execute the insert query
cur.execute(insert_query)

# Commit the changes and close the connection
conn.commit()
cur.close()
conn.close()
