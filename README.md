# Data2Bots Technical Assessment Submission

## Table of Contents
- [Overview](#overview)
- [Requirements](#requirements)
- [Technologies Used](#technologies-used)
- [Installation and Usage](#installation-and-usage)
- [File Structure](#file-structure)
- [Data Pipeline](#data-pipeline)
  - [Extraction](#extraction)
  - [Transformation](#transformation)
  - [Loading](#loading)
- [Challenges Faced](#challenges-faced)
- [Future Enhancements](#future-enhancements)
- [Conclusion](#conclusion)
- [Acknowledgements](#acknowledgements)

## Overview
- This README.md file serves as the submission for the Data2Bots Data Engineering Technical Assessment. The purpose of this assessment is to demonstrate the ability to solve a real-world data engineering problem, showcasing skills in data extraction, transformation and loading.
This project was carried out on the GitHub codespace for my flexiblity.

## Requirements
The Data2Bots Data Engineering Technical Assessment requires the following:

1. Load Data from Central Data Lake:
   - Access the central data lake, which is an Amazon S3 bucket.
   - Extract the following raw data files from the specified directory in the bucket: `orders.csv`, `reviews.csv`, and `shipments_deliveries.csv`.
   - Load the extracted data into a Postgres database within the enterprise data warehouse.
   - Create a schema in the database to hold the tables for the data engineering transformations.

2. Data Transformations:
   - Perform data transformations to derive insights and answer specific questions about the data.
   - Determine the total number of orders placed on a public holiday every month for the past year.
   - Calculate the total number of late shipments.
   - Calculate the total number of undelivered shipments.
   - Identify the product with the highest reviews, the day it was ordered the most, and additional information such as whether that day was a public holiday, total review points, percentage distribution of review points, and percentage distribution of early shipments to late shipments for that product.
   - Write SQL queries to implement the transformations and generate the required derived tables.

3. Data Loading and Export:
   - Load the derived tables into the `{your_id}_analytics` schema in the Postgres database.
   - Export specific tables to the `analytics_export` folder in the central data lake.
   - The exported tables should be stored in CSV format with the appropriate file names and directory structure.

4. Documentation:
   - Create a private Git repository to house the code and documentation for the assessment.
   - Include a README file that explains the approach taken and provides details on the implementation.
   - Document any additional requirements, considerations, or features that were included in the solution.
   - Ensure the repository is private to maintain the confidentiality of the assessment.

The assessment aims to assess the ability to build an ELT pipeline, apply data transformations, and demonstrate skills in data extraction, loading, and analysis. It is essential to adhere to best practices for scalability, maintainability, and reliability when designing and implementing the solution.

## Technologies Used
The Data2Bots Data Engineering Technical Assessment utilized the following technologies and tools:

- Python: The primary programming language used for data extraction, transformation, and loading tasks.
- SQL: Used for writing queries to perform data transformations and analysis in the Postgres database.
- Boto3: A Python library used to interact with Amazon Web Services (AWS). It was specifically used to access and download files from the S3 bucket in the data extraction phase.
- Psycopg2: A PostgreSQL adapter for Python that provides an interface to interact with the Postgres database. It was used to establish a connection to the database and execute SQL queries for data loading and transformation.
- CSV: A built-in Python library for reading and writing CSV files. It was used to handle the extraction of data from the raw CSV files before loading them into the database.

The combination of Python and SQL, along with the libraries mentioned, facilitated the implementation of the ELT pipeline, data transformations, and data loading tasks in a seamless manner.

## Installation and Usage

To set up and run the Data2Bots Data Engineering Technical Assessment project, follow the steps below:

1. Clone the repository:

   ```
   git clone <repository_url>
   ```

2. Install the required dependencies:

   ```bash
   pip install psycopg2 boto3
   ```

   The project dependencies include `psycopg2` and `boto3` libraries. You can install them using pip.

3. Configure AWS Access:

   In order to access the S3 bucket (`d2b-internal-assessment-bucket`) and export tables, make sure you have the necessary AWS access credentials configured. If you do not have access, the export functionality (`export.py`) may not work as expected.

4. Set up the database connection:

   Update the database connection details in the necessary Python files: `data_loading.py`, `agg_public_holiday.py`, `agg_shipments.py`, `best_performing_products.py`. Update the host, port, database, username, and password to match your environment.

   ```python
   # Example connection configuration in data_loading.py
   import psycopg2

   conn = psycopg2.connect(
       host="your_host",
       port="your_port",
       database="your_database",
       user="your_username",
       password="your_password"
   )
   ```

5. Run the Python scripts:

   Execute the following Python scripts in the provided order:

   - `data_loading.py`: Loads the data from the `d2b_accessment` schema to the `emmachid6410_staging` schema in the Postgres database.

   ```bash
   python data_loading.py
   ```

   - `agg_public_holiday.py`: Performs the transformation to calculate the total number of orders placed on public holidays each month and loads data to the `emmachid6410_analytics` schema.

   ```bash
   python agg_public_holiday.py
   ```

   - `agg_shipments.py`: Performs the transformation to calculate the total number of late and undelivered shipments `emmachid6410_analytics` schema.

   ```bash
   python agg_shipments.py
   ```

   - `best_performing_products.py`: Performs the transformation to identify the product with the highest reviews and additional metrics `emmachid6410_analytics` schema.

   ```bash
   python best_performing_products.py
   ```

   Make sure to have the necessary permissions and access rights to execute the Python scripts and interact with the database.

6. Export the tables (Optional):

   If you have the required access to the `d2b-internal-assessment-bucket`, you can run the `export.py` script to export the derived tables to the `analytics_export` folder in the S3 bucket. However, please note that without proper access, this functionality will not work as expected.

   ```bash
   python export.py
   ```

   Ensure you provide the correct AWS credentials and update the export functionality if necessary. If the export functionality worked successfully, the transformed tables will be exported to the analytics_export folder in the S3 bucket (d2b-internal-assessment-bucket). You can access the exported tables using the following path format:

```bash
s3://d2b-internal-assessment-bucket/analytics_export/{your_id}/<table_name>.csv
```
Replace `{your_id}` with your unique identifier and `<table_name>` with the name of the exported table.

Following the above steps will allow you to set up the project, install the dependencies, and run the provided Python scripts for data loading, transformations, and optional table export.

## File Structure
The project's file structure is organized as follows:

- `data_loading.py`: Python script that loads data from the `d2b_accessment` schema into the `emmachid6410_staging` schema in the Postgres database.

- `agg_shipments.py`: Python script that performs the transformation to calculate the total number of late and undelivered shipments. The transformed table is loaded into the `emmachid6410_analytics` schema.

- `agg_public_holiday.py`: Python script that performs the transformation to calculate the total number of orders placed on public holidays each month. The transformed table is loaded into the `emmachid6410_analytics` schema.

- `best_performing_product.py`: Python script that performs the transformation to identify the product with the highest reviews and additional metrics. The transformed table is loaded into the `emmachid6410_analytics` schema.

- `orders.csv`, `reviews.csv`, `shipment_deliveries.csv`: CSV files containing the raw data downloaded from the `emmachid6410_staging` schema. These files are used for data loading and transformations.

- `emmachid6410_analytics_agg_shipments.csv`, `emmachid6410_analytics_agg_public_holiday.csv`, `emmachid6410_analytics_agg_best_performng_products.csv`: CSV files representing the transformed tables downloaded from the `emmachid6410_analytics` schema. These files capture the results of the transformations.

- `README.md`: The README file providing a high-level and detailed overview of the repository.

- `images/`: Folder containing screenshots of error messages encountered during the exercise. 

- `aws/`: Folder containing the AWS CLI installation files. This folder is used to install the AWS CLI in the CodeSpace environment. The code snippets below show how `aws` was installed on my github codespace environement. 
```bash
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
```
```bash
sudo ./aws/install
```
```bash
aws --version
```
```bash
aws configure
```

- `extension.json`: File recommending extensions for the project. The extensions used are `grapecity.gc-excelviewer` and `ms-ossdata.vscode-postgresql`, which aid in working with Excel files and PostgreSQL, respectively.

The file structure provides a clear organization of the project's components, including the scripts for data loading and transformations, input and output data files, and supplementary files for documentation and tooling.

## Data Pipeline

The data pipeline consists of three main stages: Extraction, Transformation, and Loading.

### Extraction

The data extraction process involves retrieving the data from the source, which in this case is the Amazon S3 bucket. The `data_loading.py` file is responsible for extracting the data and loading it into the `emmachid6410_staging` schema in the Postgres database. The code snippet below demonstrates the extraction and loading of the `orders` table:

```python
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
```

### Transformation

The transformation stage involves applying SQL queries to the extracted data to derive the required insights. The transformations are performed using SQL queries written within Python using the `psycopg2` library. For detailed information and code snippets for each transformation, refer to the individual files `agg_shipments.py`, `agg_public_holiday.py`, and `best_performing_product.py` within the repository.

### Loading

The loading process involves storing the transformed data into the target folder `analytics_export/emmachid6410/` in the Postgres database. Although there were issues with loading the data to the specified location in the instructions, the attempted export from the schema to the data warehouse can be found in the `export.py` file. Due to the lack of credentials, the export functionality may not work as intended.

The data pipeline follows a sequential flow, starting from extraction, moving to transformation, and finally loading the transformed data into the target schema.

## Challenges Faced

Throughout the project, I encountered several challenges that impacted the execution and completion of certain tasks. The challenges faced include:

1. **Downloading the shipment_deliveries table**: I faced difficulties downloading the `shipment_deliveries` table directly from the schema. As a workaround, I had to download the file from the terminal using the command: 
```bash
\copy (SELECT * FROM shipment_deliveries) TO 'shipment_deliveries.csv' WITH (FORMAT csv, HEADER)
``` 
after connecting to the database using:
```bash
psql -h 34.89.230.185 -U emmachid6410 -d d2b_accessment
``` 

2. **Restricted environment**: I was limited to using GitHub Codespaces, which required additional configuration and downloads. I had to set up my Codespace environment and install necessary tools such as AWS and PostgreSQL to ensure smooth execution of the project.

3. **Loading data to the specified folder**: Unfortunately, I encountered issues while attempting to load the derived tables from the `emmachid6410_analytics` schema to the requested folder `analytics_export/emmachid6410/`. This was due to the lack of access to the specified bucket `d2b-internal-assessment-bucket`. Although I couldn't successfully load the data to the desired location, I have included the downloaded CSV files in the repository for reference.

4. **Ingestion date as primary key**: One challenge was that the documentation stated that the `ingestion_date` could be used as the primary key. However, during implementation, I discovered that it was not possible to use it as a primary key. As a solution, I used the monthly order dates as the primary key, and a separate column was added for the `ingestion_date`.

Despite these challenges, I made the best effort to work within the given constraints and provide meaningful insights from the available data sources.

## Future Enhancements

While completing this assessment, I identified several potential areas for future enhancements and improvements. These include:

1. **Modularize extraction process**: Currently, the extraction process for different tables is implemented in separate Python files. In the future, it would be beneficial to modularize the extraction process by creating a unified module or class that handles the extraction of data from different sources. This would improve code maintainability and reusability.

2. **Optimize data loading**: To improve the loading process, it would be worthwhile to explore optimizations such as batch loading or parallel loading. This would help in handling larger datasets efficiently and reducing the overall loading time.

3. **Error handling and logging**: Enhancing error handling mechanisms and implementing comprehensive logging would be valuable. This would assist in identifying and resolving any issues that may arise during the data pipeline execution, making it easier to debug and troubleshoot.

4. **Automate data ingestion**: Implementing an automated data ingestion process would ensure that new data files arriving in the S3 bucket are automatically detected and processed. This could be achieved using AWS services like AWS Lambda and AWS Glue, or by scheduling data pipeline jobs using tools like Apache Airflow.

5. **Improved documentation**: Enhancing the documentation to provide detailed explanations of each component, its purpose, and how to use it would make the repository more professional. This would include providing clear instructions on how to set up the project, execute the data pipeline, and interpret the results. Additionally, documenting any assumptions made and potential limitations would help future users understand the project better.

To make a Data Engineering repository more professional, consider the following suggestions:

- Adopt version control: Utilize a version control system like Git to track changes, collaborate with team members, and maintain a history of the project's development.

- Implement testing: Include unit tests and integration tests to validate the functionality of the data pipeline and ensure the accuracy of transformations. This helps in catching errors and provides confidence in the reliability of the pipeline.

- Use environment management: Utilize tools like virtual environments or containerization (e.g., Docker) to manage dependencies and ensure consistent development environments across different systems.

- Apply security measures: Ensure sensitive information, such as credentials and access keys, is securely managed and not exposed in the repository. Consider using environment variables or configuration files to store sensitive data.

- Include a more comprehensive README: Provide clear instructions on project setup, dependencies, execution, and any additional configuration required. Include examples and code snippets where necessary to facilitate understanding and usage.

- Follow coding and documentation standards: Adhere to coding style guidelines and document the code following standards such as PEP 8 for Python. This promotes consistency and improves readability for other developers who may review or contribute to the project.

By incorporating these enhancements and best practices, the Data Engineering repository can become more professional, maintainable, and user-friendly. All these and more could have been implemented if the time factor was more elastic.

## Conclusion

In conclusion, this project has been a valuable learning experience that has challenged me and provided opportunities for growth. Through the Data2Bots Technical Assessment, I gained hands-on experience in developing a data pipeline to extract, transform, and load data from a source into a target database. I encountered various challenges along the way, which allowed me to sharpen my problem-solving skills and expand my knowledge of data engineering practices.

By completing this assessment, I achieved the following outcomes:

- **Data Extraction**: I successfully extracted data from the source, an Amazon S3 bucket, using Python and SQL queries. I leveraged the psycopg2 library to establish a connection to the database and retrieve the required tables.

- **Data Transformation**: I performed data transformations using SQL queries embedded within Python code. This involved aggregating data, applying filters, and deriving new insights. The transformations were implemented in separate Python files for modularity and maintainability.

- **Data Loading**: Although I encountered challenges in loading data to the desired location due to access restrictions, I developed the necessary code using the export.py file to export data from the staging schema to the emmachid6410_analytics schema.

Throughout this project, I learned the importance of meticulous data handling, maintaining data integrity, and ensuring the reliability of the data pipeline. I also gained experience in troubleshooting issues and finding alternative solutions when faced with limitations or constraints.

Moving forward, I plan to further enhance this project by implementing the suggested future enhancements mentioned earlier. This will enable me to optimize the extraction, transformation, and loading processes, automate data ingestion, and improve the overall professionalism of the repository.

In conclusion, the Data2Bots Technical Assessment provided me with practical insights into data engineering and reinforced the significance of robust data pipelines for effective data management. I look forward to applying the knowledge and skills gained from this experience in future data engineering projects.

## Acknowledgements

I would like to express my gratitude to the following resources and communities that have contributed to the successful completion of this project:

- **Psycopg2 Documentation**: The official documentation of Psycopg2 has been an invaluable resource throughout this project. It provided comprehensive information and examples on how to connect to PostgreSQL databases, execute SQL queries, and handle data effectively. The documentation helped me understand the intricacies of using Psycopg2 and allowed me to leverage its capabilities efficiently.

- **Stack Overflow**: The Stack Overflow community played a significant role in resolving various challenges encountered during the development process. The platform provided a wealth of knowledge and insights shared by experienced developers. Many questions and answers on topics related to data extraction, transformation, and loading were instrumental in overcoming roadblocks and finding effective solutions.
