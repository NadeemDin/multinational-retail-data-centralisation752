from data_cleaning import DataCleaning
from data_extraction import DataExtractor
from database_utils import DatabaseConnector
from datetime import datetime
import pandas as pd

if __name__ == "__main__":
    # Initialize the database connector
    db_connector = DatabaseConnector()

    # List the tables in the connected AWS RDS database
    #db_connector.list_db_tables()

    # Proceed with data cleaning of AWS RDS
    #cleaner = DataCleaning()
    #cleaned_data = cleaner.clean_data()

    # Read data from PDF using DataExtractor
    '''ext = DataExtractor()
    pdf_link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
    pdf_data = ext.retrieve_pdf_data(pdf_link)

    cleaned_card_data = cleaner.clean_card_data(pdf_data)
    print(cleaned_card_data)'''

    '''try:
        cleaned_card_data.to_excel('output_card_data.xlsx', index=True)
        print("Card data output written to 'output_card_data.xlsx'")
    except Exception as e:
        print(f"Error writing card data output to Excel: {e}")'''


    # convert RDS pandas file to to excel to view
    '''try:
        cleaned_data.to_excel('output_final.xlsx', index=True)
        print("Output written to 'output_final.xlsx'")
    except Exception as e:
        print(f"Error writing output to Excel: {e}")'''

    # Upload to PostgreSQL database
    '''print("uploading")
    table_name = 'dim_card_details'
    db_connector.upload_db(cleaned_card_data, table_name)
    print(f"Data uploaded to {table_name} in PostgreSQL database.")'''

    # pull number of stores from api (451 stores) may only be 450 due to indexing
    '''data_extractor = DataExtractor()
    api_key = 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'
    header_dict = {'x-api-key': api_key}
    number_of_stores_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'

    result = data_extractor.list_number_of_stores(number_of_stores_endpoint, header_dict)

    if result is not None:
        print(f"Number of stores: {result}")
    else:
        print("Failed to retrieve the number of stores.")'''
    '''
    #extract store data + clean
    data_extractor = DataExtractor()
    cleaner = DataCleaning()
    api_key = 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'
    header_dict = {'x-api-key': api_key}
    store_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{}'

    # Retrieving stores data
    stores_data_df = data_extractor.retrieve_stores_data(store_endpoint, header_dict)

    if stores_data_df is not None:
        print("Original Stores Data:")
        print(stores_data_df)
        stores_data_df.to_excel('stores_pre.xlsx', index=True)
        print("Output written to 'excel")
        

        # Cleaning the stores data
        cleaned_store_df = cleaner.clean_store_data(stores_data_df)
        

        if cleaned_store_df is not None:
            print("\nCleaned Stores Data:")
            print(cleaned_store_df)
        else:
            print("\nFailed to clean stores data.")
    else:
        print("Failed to retrieve stores data.")


    cleaned_store_df.to_excel('stores.xlsx', index=True)
    print("Output written to 'stores.xlsx'")

    print("uploading")
    table_name = 'dim_store_details'
    db_connector.upload_db(cleaned_store_df, table_name)
    print(f"Data uploaded to {table_name} in PostgreSQL database.")'''


    #s3 product extraction and clean
    '''data_extractor = DataExtractor()
    cleaner = DataCleaning()
    s3_address = 's3://data-handling-public/products.csv'

    products_df = data_extractor.extract_from_s3(s3_address)

    if products_df is not None:
        print("Products DataFrame:")
        print(products_df)
    else:
        print("Failed to extract data from S3.")


    # Cleaning the prod data
    products_df = cleaner.clean_prod_data(products_df)
    print("Products DataFrame:")
    print(products_df)
    
        
    products_df.to_excel('prods.xlsx', index=True)
    print("Output written to 'prods.xlsx'")    

    print("uploading")
    table_name = 'dim_products'
    db_connector.upload_db(products_df, table_name)
    print(f"Data uploaded to {table_name} in PostgreSQL database.")'''



    # Assuming 'products_df' is the DataFrame containing product information
    #cleaned_products_df = data_cleaner.convert_product_weights(products_df)

    '''if products_df is not None:
        print("Cleaned Products DataFrame:")
        print(products_df)
        products_df.to_excel('prods.xlsx', index=True)
        print("Output written to 'prods.xlsx'")

    else:
        print("Failed to convert product weights.") '''  


    #ORDERS TABLE RDS EXTRACTIONS:
    '''
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"Current time: {current_time}")
    extractor = DataExtractor()
    cleaner = DataCleaning()
    orders_df = extractor.read_rds_table('orders_table')
    print(type(orders_df))
    orders_df = cleaner.clean_order_data(orders_df)


    
    if orders_df is not None:
        print("Cleaned DataFrame:")
        print(orders_df)
        orders_df.to_excel('orders.xlsx', index=True)
        print("Output written to 'orders.xlsx'")

    print("uploading")
    table_name = 'orders_table'
    df = orders_df
    db_connector.upload_db(df, table_name)
    print(f"Data uploaded to {table_name} in PostgreSQL database.")'''

    #date data from json:
    '''
    #s3 product extraction and clean
    data_extractor = DataExtractor()
    cleaner = DataCleaning()
    json_url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'

    json_df = data_extractor.extract_json(json_url)

    


    # Cleaning the  data
    print("cleaning")
    json_df = cleaner.clean_json_data(json_df)
    print("Products DataFrame:")
    print(json_df)

    # Check the result
    if json_df is not None:
        print(json_df.head())
        json_df.to_excel('date_data.xlsx', index=True)
        print("Output written to 'date_data.xlsx'")

    print("uploading")
    table_name = 'dim_date_times'
    df = json_df
    db_connector.upload_db(df, table_name)
    print(f"Data uploaded to {table_name} in PostgreSQL database.")'''


    

    
        

