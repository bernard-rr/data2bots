import psycopg2
import boto3
import csv

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

# Create the orders table in the staging schema if it doesn't exist
cur.execute("""
    CREATE TABLE IF NOT EXISTS emmachid6410_staging.orders (
        order_id INT PRIMARY KEY,
        customer_id INT NOT NULL,
        order_date DATE NOT NULL,
        product_id INT NOT NULL,
        unit_price DECIMAL(10, 2) NOT NULL,
        quantity INT NOT NULL,
        amount DECIMAL(10, 2) NOT NULL
    );
""")

# Commit the table creation
conn.commit()

# Load orders.csv into the orders table
with open('orders.csv', 'r') as file:
    reader = csv.reader(file)
    next(reader)  # Skip header row
    for row in reader:
        order_id, customer_id, order_date, product_id, unit_price, quantity, amount = row
        query = "INSERT INTO emmachid6410_staging.orders (order_id, customer_id, order_date, product_id, unit_price, quantity, amount) VALUES (%s, %s, %s, %s, %s, %s, %s)"
        cur.execute(query, (order_id, customer_id, order_date, product_id, unit_price, quantity, amount))

cur.execute("""
    CREATE TABLE IF NOT EXISTS emmachid6410_staging.reviews (
        review INT NOT NULL,
        product_id INT NOT NULL
    );
""")

# Commit the table creation
conn.commit()

# Load reviews.csv into the reviews table
with open('reviews.csv', 'r') as file:
    reader = csv.reader(file)
    next(reader)  # Skip header row
    for row in reader:
        review, product_id = row
        query = "INSERT INTO emmachid6410_staging.reviews (review, product_id) VALUES (%s, %s)"
        cur.execute(query, (review, product_id))

# Create the shipments_deliveries table if it doesn't exist
cur.execute("""
    CREATE TABLE IF NOT EXISTS emmachid6410_staging.shipments_deliveries (
        shipment_id INT NOT NULL PRIMARY KEY,
        order_id INT NOT NULL,
        shipment_date DATE,
        delivery_date DATE
    );
""")

# Commit the table creation
conn.commit()

# Load shipments_deliveries.csv into the shipments_deliveries table
with open('/workspaces/data2bots/shipment_deliveries.csv', 'r') as file:
    reader = csv.reader(file)
    next(reader)  # Skip header row
    for row in reader:
        shipment_id, order_id, shipment_date, delivery_date = row
        if shipment_date == '':
            shipment_date = None  # Convert empty string to None (NULL)
        if delivery_date == '':
            delivery_date = None  # Convert empty string to None (NULL)
        query = "INSERT INTO emmachid6410_staging.shipments_deliveries (shipment_id, order_id, shipment_date, delivery_date) VALUES (%s, %s, %s, %s)"
        cur.execute(query, (shipment_id, order_id, shipment_date, delivery_date))

# Commit the changes and close the connection
conn.commit()
cur.close()
conn.close()
print("Data updated Successfully")
