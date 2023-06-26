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

cur.execute("""
    CREATE TABLE IF NOT EXISTS emmachid6410_analytics.agg_public_holiday (
        order_date DATE NOT NULL PRIMARY KEY,
        ingestion_date DATE NOT NULL,
        tt_order_hol_jan INT NOT NULL,
        tt_order_hol_feb INT NOT NULL,
        tt_order_hol_mar INT NOT NULL,
        tt_order_hol_apr INT NOT NULL,
        tt_order_hol_may INT NOT NULL,
        tt_order_hol_jun INT NOT NULL,
        tt_order_hol_jul INT NOT NULL,
        tt_order_hol_aug INT NOT NULL,
        tt_order_hol_sep INT NOT NULL,
        tt_order_hol_oct INT NOT NULL,
        tt_order_hol_nov INT NOT NULL,
        tt_order_hol_dec INT NOT NULL
    );
""")

# Truncate the agg_public_holiday table before populating new data
truncate_query = sql.SQL("TRUNCATE TABLE emmachid6410_analytics.agg_public_holiday")
cur.execute(truncate_query)

# Populate the agg_public_holiday table with the total number of orders on public holidays for each month
insert_query = sql.SQL("""
    INSERT INTO emmachid6410_analytics.agg_public_holiday (order_date, ingestion_date, tt_order_hol_jan, tt_order_hol_feb, tt_order_hol_mar, tt_order_hol_apr,
                                                           tt_order_hol_may, tt_order_hol_jun, tt_order_hol_jul, tt_order_hol_aug, tt_order_hol_sep,
                                                           tt_order_hol_oct, tt_order_hol_nov, tt_order_hol_dec)
    SELECT
        date_trunc('month', o.order_date) AS order_date,
        CURRENT_DATE AS ingestion_date,
        COUNT(CASE WHEN extract(month from o.order_date) = 1 AND d.day_of_the_week_num >= 1 AND d.day_of_the_week_num <= 5 AND d.working_day = false THEN 1 END) AS tt_order_hol_jan,
        COUNT(CASE WHEN extract(month from o.order_date) = 2 AND d.day_of_the_week_num >= 1 AND d.day_of_the_week_num <= 5 AND d.working_day = false THEN 1 END) AS tt_order_hol_feb,
        COUNT(CASE WHEN extract(month from o.order_date) = 3 AND d.day_of_the_week_num >= 1 AND d.day_of_the_week_num <= 5 AND d.working_day = false THEN 1 END) AS tt_order_hol_mar,
        COUNT(CASE WHEN extract(month from o.order_date) = 4 AND d.day_of_the_week_num >= 1 AND d.day_of_the_week_num <= 5 AND d.working_day = false THEN 1 END) AS tt_order_hol_apr,
        COUNT(CASE WHEN extract(month from o.order_date) = 5 AND d.day_of_the_week_num >= 1 AND d.day_of_the_week_num <= 5 AND d.working_day = false THEN 1 END) AS tt_order_hol_may,
        COUNT(CASE WHEN extract(month from o.order_date) = 6 AND d.day_of_the_week_num >= 1 AND d.day_of_the_week_num <= 5 AND d.working_day = false THEN 1 END) AS tt_order_hol_jun,
        COUNT(CASE WHEN extract(month from o.order_date) = 7 AND d.day_of_the_week_num >= 1 AND d.day_of_the_week_num <= 5 AND d.working_day = false THEN 1 END) AS tt_order_hol_jul,
        COUNT(CASE WHEN extract(month from o.order_date) = 8 AND d.day_of_the_week_num >= 1 AND d.day_of_the_week_num <= 5 AND d.working_day = false THEN 1 END) AS tt_order_hol_aug,
        COUNT(CASE WHEN extract(month from o.order_date) = 9 AND d.day_of_the_week_num >= 1 AND d.day_of_the_week_num <= 5 AND d.working_day = false THEN 1 END) AS tt_order_hol_sep,
        COUNT(CASE WHEN extract(month from o.order_date) = 10 AND d.day_of_the_week_num >= 1 AND d.day_of_the_week_num <= 5 AND d.working_day = false THEN 1 END) AS tt_order_hol_oct,
        COUNT(CASE WHEN extract(month from o.order_date) = 11 AND d.day_of_the_week_num >= 1 AND d.day_of_the_week_num <= 5 AND d.working_day = false THEN 1 END) AS tt_order_hol_nov,
        COUNT(CASE WHEN extract(month from o.order_date) = 12 AND d.day_of_the_week_num >= 1 AND d.day_of_the_week_num <= 5 AND d.working_day = false THEN 1 END) AS tt_order_hol_dec
    FROM
        emmachid6410_staging.orders o
        JOIN if_common.dim_dates d ON o.order_date = d.calendar_dt
    GROUP BY
        date_trunc('month', o.order_date)
""")
cur.execute(insert_query)

# Commit the changes and close the database connection
conn.commit()
cur.close()
conn.close()