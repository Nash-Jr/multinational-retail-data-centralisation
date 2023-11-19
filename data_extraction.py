import pandas as pd
from sqlalchemy import create_engine, MetaData, Table
import boto3
import requests
from DatabaseConnector import DatabaseConnector


class DataExtractor:
    """
    Class for extracting and managing data from various sources.

    Parameters:
    - db_connector: An instance of DatabaseConnector for database connectivity.
    """

    def __init__(self, db_connector):
        """
        Initialise DataExtractor object.

        Parameters:
        - db_connector: An instance of DatabaseConnector for database connectivity.
        """
        self.db_connector = db_connector
        self.rds_engine = db_connector.init_db_engine()
        self.metadata = MetaData()
        self.metadata.reflect(bind=self.rds_engine)
        self.df = None
        self.extract_df = None
        self.stores_dataframe = None
        self.final_df = None

    def init_db_engine(self):
        """
        Initialise the database engine.

        Returns:
        - engine: SQLAlchemy engine object for database connectivity.
        """
        db_credentials = self.db_connector.read_db_creds(
            r"C:\Users\nacho\New folder\AiCore\multinational-retail-data-centralisation\db_creds.yml")
        engine = create_engine(
            f"postgresql://{db_credentials['RDS_USER']}:{db_credentials['RDS_PASSWORD']}@{db_credentials['RDS_HOST']}:{db_credentials['RDS_PORT']}/{db_credentials['RDS_DATABASE']}"
        )
        return engine

    def list_db_tables(self):
        """
        List the names of tables in the connected database.

        Returns:
        - table_names: List of table names.
        """
        table_names = self.metadata.tables.keys()
        return table_names

    def read_rds_table(self, table_name):
        """
        Read data from a specified table in the connected database.

        Parameters:
        - table_name: Name of the table to read data from.

        Returns:
        - df: Pandas DataFrame containing the table data.
        """
        rds_table = Table(table_name, self.metadata,
                          autoload_with=self.rds_engine)
        query = rds_table.select()
        with self.rds_engine.connect() as connection:
            result = connection.execute(query)
            data = result.fetchall()
        df = pd.DataFrame(data, columns=result.keys())
        return df

    def extract_from_s3(self, s3_address):
        """
        Extract data from an S3 bucket.

        Parameters:
        - s3_address: S3 address specifying the location of the data.

        Returns:
        - extract_df: Pandas DataFrame containing the extracted data.
        """
        s3_parts = s3_address.replace('s3://', '').split('/')
        bucket_name, file_key = s3_parts[0], '/'.join(s3_parts[1:])
        s3 = boto3.client('s3')
        try:
            obj = s3.get_object(Bucket=bucket_name, Key=file_key)
            extract_df = pd.read_csv(obj['Body'])
            return extract_df
        except Exception as e:
            print(f"Error extracting data from S3: {e}")
            return pd.DataFrame()

    def list_number_of_stores(self, number_pf_stores_endpoint):
        """
        Retrieve the number of stores from an API endpoint.

        Parameters:
        - number_pf_stores_endpoint: API endpoint for retrieving store information.

        Returns:
        - number_of_stores: Number of stores retrieved from the API.
        """
        api_key = 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'
        headers = {'x-api-key': api_key}
        response = requests.get(number_pf_stores_endpoint, headers=headers)
        try:
            if response.status_code == 200:
                number_of_stores = response.json()
                return number_of_stores
            else:
                print(
                    f"Failed to retrieve the number of stores. Status code: {response.status_code}")
                return None
        except Exception as e:
            print(f"An error occurred: {e}")
            return None

    def retrieve_stores_data(self, api_endpoint):
        """
        Retrieve stores data from an API endpoint.

        Parameters:
        - api_endpoint: API endpoint for retrieving store information.

        Returns:
        - stores_dataframe: Pandas DataFrame containing store information.
        """
        response = requests.get(api_endpoint)
        if response.status_code == 200:
            stores_data = response.json()
            stores_list = stores_data.get('stores', [])
            stores_dataframe = pd.DataFrame(stores_list)
            return stores_dataframe
        else:
            print(f'ERROR: unable to retrieve data from {api_endpoint}')
            print(response.content)
            return None

    def retrieve_sales_date(self, url):
        """
        Retrieve sales date data from a specified URL.

        Parameters:
        - url: URL for retrieving sales date information.

        Returns:
        - sales_date_df: Pandas DataFrame containing sales date information.
        """
        try:
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                sales_date_df = pd.DataFrame(data)
                return sales_date_df
            else:
                print(
                    f"Failed to retrieve data. Status code: {response.status_code}")
                return None
        except Exception as e:
            print(f"An error occurred: {e}")
            return None


db_credentials = {
    'config_file_path': r"C:\Users\nacho\New folder\AiCore\multinational-retail-data-centralisation\db_creds.yml"
}

db_connector = DatabaseConnector(**db_credentials)
data_extractor = DataExtractor(db_connector)
number_of_stores_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
headers = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
result = data_extractor.retrieve_stores_data(number_of_stores_endpoint)
print(result)
