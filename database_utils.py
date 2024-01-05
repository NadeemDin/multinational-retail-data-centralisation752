import pymysql

class DatabaseConnector:
    def __init__(self, host, user, password, database):
        self.connection = pymysql.connect(host=host, user=user, password=password, database=database)

    def upload_to_database(self, data):
        # Method to upload data to the database
        # Implement your logic here
        pass

    def close_connection(self):
        self.connection.close()
