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



# Create the table if it does not exist
cur.execute("""
    CREATE TABLE IF NOT EXISTS emmachid6410_analytics.agg_shipments (
        order_date DATE NOT NULL PRIMARY KEY,
        ingestion_date DATE NOT NULL,
        tt_late_shipments INT NOT NULL,
        tt_undelivered_items INT NOT NULL
    )
""")


# Truncate the agg_public_holiday table before populating new data
truncate_query = sql.SQL("TRUNCATE TABLE emmachid6410_analytics.agg_shipments")
cur.execute(truncate_query)

# Perform the joins and insert the data into the table
cur.execute("""
    INSERT INTO emmachid6410_analytics.agg_shipments (
        order_date,
        ingestion_date,
        tt_late_shipments,
        tt_undelivered_items
    )
    SELECT
        date_trunc('month', o.order_date) AS order_date,
        CURRENT_DATE AS ingestion_date,
        COUNT(*) FILTER (WHERE s.shipment_date >= (o.order_date + INTERVAL '6 days') AND s.delivery_date IS NULL) AS tt_late_shipments,
        COUNT(*) FILTER (WHERE s.delivery_date IS NULL AND s.shipment_date IS NULL AND DATE '2022-09-05' >= (o.order_date + INTERVAL '15 days')) AS tt_undelivered_items
    FROM
        emmachid6410_staging.orders o
    INNER JOIN
        emmachid6410_staging.shipments_deliveries AS s
    ON
        o.order_id = s.order_id
    GROUP BY
        date_trunc('month', o.order_date)
""")

# Commit the changes and close the connection
conn.commit()
cur.close()
conn.close()