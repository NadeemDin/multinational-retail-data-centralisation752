from data_cleaning import DataCleaning
from data_extraction import DataExtractor
from database_utils import DatabaseConnector

if __name__ == "__main__":
    # Initialize the database connector
    db_connector = DatabaseConnector()

    # List the tables in the connected database
    db_connector.list_db_tables()

    # Proceed with data cleaning
    cleaner = DataCleaning()
    cleaned_data = cleaner.clean_data()

    # ... (rest of your main function)

    '''try:
        cleaned_data.to_excel('output_final.xlsx', index=True)
        print("Output written to 'output_final.xlsx'")
    except Exception as e:
        print(f"Error writing output to Excel: {e}")'''

    # Upload to PostgreSQL database
    table_name = 'dim_users'
    db_connector.upload_db(cleaned_data, table_name)
    print(f"Data uploaded to {table_name} in PostgreSQL database.")
