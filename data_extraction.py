'''This class will work as a utility class, 
in it you will be creating methods that help extract data from different data sources.
The methods contained will be fit to extract data from 
a particular data source, these sources will include CSV files, an API and an S3 bucket.
'''

import pandas as pd
from sqlalchemy import text
from database_utils import DatabaseConnector
from tabula import read_pdf
import requests
import boto3
from io import BytesIO

class DataExtractor:
    def __init__(self):
        pass

    def read_rds_table(self, table_name):
        # Connect to the database to pull data from table name= 
        connector = DatabaseConnector()
        engine = connector.init_db_engine()

        # Read the table into a pandas DataFrame
        query = f"SELECT * FROM {table_name}"
        
        with engine.begin() as conn:
            df = pd.read_sql_query(sql=text(query), con=conn)
      
        return df
        
    def retrieve_pdf_data(self, pdf_link):
        try:
            # Use tabula to extract data from the PDF
            pdf_data = read_pdf(pdf_link, pages='all', multiple_tables=True)

            # Concatenate the extracted tables into a single DataFrame
            pdf_df = pd.concat(pdf_data, ignore_index=True)

            pdf_df.reset_index(drop=True, inplace=True)
            pdf_df.index.name = 'index'

            return pdf_df
        except Exception as e:
            print(f"Error extracting data from PDF: {e}")
            return None

    def list_number_of_stores(self, number_of_stores_endpoint, headers):
        try:
            response = requests.get(number_of_stores_endpoint, headers=headers)

            print(f"API Response Status Code: {response.status_code}")
            print(f"API Response Content: {response.content}")

            if response.status_code == 200:
                # Assuming the API response contains the number of stores in a 'number_stores' field
                number_of_stores = response.json().get('number_stores')
                return number_of_stores
            else:
                print(f"Failed to retrieve the number of stores. Status Code: {response.status_code}")
                return None
        except Exception as e:
            print(f"Error in list_number_of_stores: {e}")
            return None
            #returns 451 
        
    def retrieve_stores_data(self, store_endpoint, headers, total_stores=451):
        try:
            stores_data = []

            # Loop through each store number and retrieve store details
            for store_number in range(1, total_stores + 1):
                store_url = store_endpoint.format(store_number)
                response = requests.get(store_url, headers=headers)

                if response.status_code == 200:
                    # Assuming the API response contains store details
                    store_details = response.json()
                    stores_data.append(store_details)
                else:
                    print(f"Failed to retrieve store {store_number}. Status Code: {response.status_code}")

            # Convert the list of dictionaries to a Pandas DataFrame
            df = pd.DataFrame(stores_data)
            return df
        except Exception as e:
            print(f"Error in retrieve_stores_data: {e}")
            return None

    def extract_from_s3(self, s3_address):
        try:
            # Assuming you are logged into AWS CLI
            s3 = boto3.client('s3')

            # Extract bucket and key from the S3 address
            bucket, key = s3_address.replace('s3://', '').split('/', 1)

            # Download the file from S3
            response = s3.get_object(Bucket=bucket, Key=key)
            data = response['Body'].read()

            # Convert the byte data to a pandas DataFrame
            df = pd.read_csv(BytesIO(data))

            return df
        except Exception as e:
            print(f"Error extracting data from S3: {e}")
            return None 


    def extract_json(self, json_url):
        try:
            # Read JSON directly into a DataFrame
            json_df = pd.read_json(json_url)
            return json_df
        except Exception as e:
            print(f"Error extracting JSON data: {e}")
            return None

# Example Usage:
