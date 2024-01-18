# Project: Multinational Retail Data Centralisation 

#### Introduction
A Retail Data Integration and Warehousing Project:

This project is designed to streamline the process of consolidating and centralising multinational retail-related data from diverse sources, enabling efficient data analysis and business insights. 
The primary goal is to create a comprehensive and well-organized data repository that facilitates informed decision-making for retail businesses operating in multiple countries.

## Project Overview
In the ever-evolving landscape of the retail industry, businesses often deal with data scattered across different platforms and formats. This project addresses the challenges associated with data fragmentation by implementing a robust Extract, Transform, Load (ETL) process.
The key components of the project include:

**1. Data Extraction**: Retrieve retail data from various sources, including cloud databases, PDF documents, APIs, CSV files, and JSON endpoints. These sources provide valuable insights into the company's operations in different countries.

**2. Data Transformation**: Clean, format, and standardise the extracted data to ensure consistency and compatibility with the target PostgreSQL database. This phase involves handling discrepancies, missing values, and aligning data structures.

**3. Data Loading**: Upload the cleaned and transformed data into a local PostgreSQL database. The database serves as a centralized repository for all retail-related information, supporting efficient data retrieval and analysis



## Project descriptions:
**1. main.py**: The main entry point of the data processing pipeline. This script orchestrates the extraction of data from various sources, data cleaning, and uploading cleaned data to a local PostgreSQL database. It is designed to handle retail-related data from different formats such as PDFs, APIs, CSVs, JSON, and cloud databases.

**2. data_extraction.py**: This module provides functionality to extract data from different sources. It includes methods to read tables from an AWS RDS database, retrieve data from PDFs, extract data from an S3 bucket, and obtain information from APIs.

**3. data_utils.py**: This module contains a DatabaseConnector class responsible for managing the connection to PostgreSQL databases. It includes methods to read database credentials from a YAML file, initialize a database engine, list tables in a connected database, and upload DataFrames to specific tables.

**4. data_cleaning.py**: This module defines the DataCleaning class, which encapsulates various methods for cleaning and transforming data. It covers tasks such as handling null values, duplicate entries, converting data types, cleaning phone numbers and addresses, and applying specific patterns to filter out unwanted data. Additionally, there are methods for cleaning data from PDFs and APIs.

**5. postgres.sales.session.sql**: The "postgres.sales.session.sql" file is a SQL script that orchestrates diverse tasks spanning multiple milestones in a data management. In Milestone 3, the script concentrates on refining data types, adjusting maximum lengths for specific columns, and implementing key constraints across various tables. It meticulously addresses data consistency in tables such as orders_table, dim_users, dim_store_details, and dim_products. Transitioning into Milestone 4, the script undertakes analytical queries, uncovering insights into store distribution, sales performance, and average time between orders. Serving as a versatile tool, it seamlessly manages and analyzes relational database data, enhancing overall system integrity and fostering a deeper understanding of the underlying dataset.



## Installation & Prerequisites:
Python packages required:
```
pip install pandas sqlalchemy tabula-py requests boto3 PyYAML
```

To interact with AWS services in your Python scripts, you need to set up AWS credentials and make sure the necessary AWS SDKs are installed. Here are the AWS prerequisites:

**AWS Credentials**:

- Ensure you have an AWS account.
Set up AWS CLI with the necessary access and secret keys by running aws configure in your terminal. This configures your AWS CLI with the required credentials.
Boto3 (Python SDK for AWS):

- Install the boto3 package using pip install boto3 to enable your Python scripts to interact with AWS services.
Amazon S3 Access:

- If you're interacting with S3, make sure your AWS credentials have the necessary permissions to read from or write to S3 buckets.
Amazon RDS Access:

- If you're interacting with Amazon RDS, ensure your AWS credentials have the necessary permissions to connect to and query the RDS database.
Ensure your AWS credentials are properly configured, and the IAM user associated with these credentials has the required permissions for the AWS services you're using in your scripts. This includes permissions for S3, RDS, and any other AWS services your scripts interact with.

My credentials will not be uploaded to this repo to ensure my account security, the data_utils.py file itself can be reconfigued to accept a new .yaml file in the format, with your credetials:

```
#AWS RDS CREDENTIALS
RDS_HOST: 
RDS_PASSWORD: 
RDS_USER: 
RDS_DATABASE: 
RDS_PORT: 

#for upload to postgres
PG_DATABASE_TYPE: 
PG_DBAPI:
PG_HOST: 
PG_USER: 
PG_PASSWORD: 
PG_DATABASE: 
PG_PORT: 
```

You may need to also create a new local postgresql database. you will upload to this via its credentials.

## Usage Instructions:
1. Clone repository to your local machine.
2. Ensure all prerequisites have been completed and youre logged into the correct AWS services.
3. Ensure your credentials are correct.
4. Within the main.py file uncomment the desired code for the required action. (please read the uncommented code before running, this may write an excel file to your directory or even upload to your postgresql db).
5. The 'postgres.sales.session.sql' file contains a list of queries which were used to extract insights from the data, copy and paste them into pgadmin or within a sql session if youre connected to your database in Vscode.

##### MULTINATIONAL RETAIL DATA CENTRALISATION project completed as part of the AICORE cloud engineering specialisation certification.

##### Personal note: I could have created a few more functions/methods to call upon within my main.py file, such as the export to excel (which i used to examine the data before, between and after cleaning, before upload), however i chose not to just so i could further practice my code and understand how things work.




