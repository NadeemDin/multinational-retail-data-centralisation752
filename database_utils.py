'''to connect and upload to db
'''
import yaml
from sqlalchemy import create_engine, inspect

class DatabaseConnector:
    def __init__(self):
        pass

  
    def read_db_creds(self, creds_file_path='db_creds.yaml'):
        # reads yaml file and returns as dict
        with open(creds_file_path, 'r') as file:
            creds = yaml.safe_load(file)
            # print(creds)
            # print(type(creds))
        return creds

    def init_db_engine(self):
        # takes credentials and creates URL to initialise db engine
        creds = self.read_db_creds()
        db_url = f"postgresql+psycopg2://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}"
        engine = create_engine(db_url)
        # print(type(engine))
        return engine

    def list_db_tables(self):
        # Connect to the database
        engine = self.init_db_engine()
        
        # Create inspector
        insp = inspect(engine)

        # Get the list of table names
        table_names = insp.get_table_names()
        
        # Print or return the list of table names
        print("Tables in the database:")
        for table_name in table_names:
            print(table_name)

# Example usage:
# connector = DatabaseConnector()
# connector.list_db_tables()
''' returns 
Tables in the database:
legacy_store_details
legacy_users        
orders_table'''