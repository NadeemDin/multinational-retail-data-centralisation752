'''This class will work as a utility class, 
in it you will be creating methods that help extract data from different data sources.
The methods contained will be fit to extract data from 
a particular data source, these sources will include CSV files, an API and an S3 bucket.
'''

import pandas as pd
from sqlalchemy import text
from database_utils import DatabaseConnector

class DataExtractor:
    def __init__(self):
        pass

    def read_rds_table(self, table_name='legacy_users'):
        # Connect to the database to pull data from table name= 
        connector = DatabaseConnector()
        engine = connector.init_db_engine()

        # Read the table into a pandas DataFrame
        query = f"SELECT * FROM {table_name}"
        
        with engine.begin() as conn:
            df = pd.read_sql_query(sql=text(query), con=conn)
           
                
        return df
    
#ext = DataExtractor()
#test = ext.read_rds_table()
#print(test)


