import psycopg2
import boto3

# S3 bucket details
s3_bucket = 'd2b-internal-assessment-bucket'
s3_folder = 'analytics_export/emmachid6410/'

# Tables to export
tables = [
    'emmachid6410_analytics.best_performing_product',
    'emmachid6410_analytics.agg_public_holiday',
    'emmachid6410_analytics.agg_shipments'
]

# Connect to the database
conn = psycopg2.connect(
    database="d2b_accessment",
    user="emmachid6410",
    password="pGeOqdZMAF",
    host="34.89.230.185",
    port=5432)
print("Database connected successfully")

# Export tables to CSV files

for table in tables:
    filename = f'{table.replace(".", "_")}.csv'
    with open(filename, 'w') as file:
        cur = conn.cursor()
        # Use the COPY TO SQL command to export the table to a CSV file
        copy_query = f"COPY {table} TO STDOUT WITH CSV HEADER"
        cur.copy_expert(copy_query, file)
        cur.close()
        print(f"Table {table} exported to {filename}")

    # Upload the CSV file to S3
    s3_key = f'{s3_folder}{filename}'
    s3 = boto3.client('s3')
    s3.upload_file(filename, s3_bucket, s3_key)
    print(f"Table {table} uploaded to S3: s3://{s3_bucket}/{s3_key}")

# Close the database connection
conn.close()