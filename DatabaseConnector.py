import yaml
from sqlalchemy import create_engine, MetaData, Table
import requests
import tabula
import argparse
import pandas as pd
import data_cleaning


class DatabaseConnector:
    """
    Class for connecting to a database, retrieving PDF data, and uploading data to the database.

    Methods:
    - __init__: Initialize the DatabaseConnector instance.
    - init_db_engine: Initialize the database engine.
    - read_db_creds: Read database credentials from a YAML file.
    - list_db_tables: List the tables in the connected database.
    - retrieve_pdf_data: Retrieve data from a PDF linked in a URL.
    - upload_to_db: Upload dataframes to the connected database.
    - some_other_method: An example method demonstrating how to use the DataCleaning class and upload data to the database.
    - from_yaml: Create an instance of DatabaseConnector from a YAML file.

    """

    def __init__(self, config_file_path=r"C:\Users\nacho\New folder\AiCore\multinational-retail-data-centralisation\db_creds.yml"):
        """
        Initialize the DatabaseConnector instance.

        Parameters:
        - config_file_path: Path to the YAML file containing database credentials.
        """
        self.config_file_path = config_file_path
        self.db_credentials = self.read_db_creds()
        self.db_engine = self.init_db_engine()

    def init_db_engine(self):
        """
        Initialize the database engine.

        Returns:
        - engine: SQLAlchemy database engine.
        """
        engine = create_engine(
            f"postgresql://{self.db_credentials['RDS_USER']}:{self.db_credentials['RDS_PASSWORD']}@{self.db_credentials['RDS_HOST']}:{self.db_credentials['RDS_PORT']}/{self.db_credentials['RDS_DATABASE']}"
        )
        return engine

    def read_db_creds(self):
        """
        Read database credentials from a YAML file.

        Returns:
        - db_credentials: Dictionary containing database credentials.
        """
        with open(self.config_file_path, 'r') as file:
            try:
                data = yaml.safe_load(file)
                return {
                    'RDS_HOST': data['RDS_HOST'],
                    'RDS_PASSWORD': data['RDS_PASSWORD'],
                    'RDS_USER': data['RDS_USER'],
                    'RDS_DATABASE': data['RDS_DATABASE'],
                    'RDS_PORT': data['RDS_PORT']
                }
            except yaml.YAMLError as e:
                print(f"Error reading YAML file {self.config_file_path}: {e}")
                return None

    def list_db_tables(self):
        """
        List the tables in the connected database.

        Returns:
        - table_names: List of table names in the database.
        """
        metadata = MetaData()
        metadata.reflect(bind=self.db_engine)
        table_names = metadata.tables.keys()
        return table_names

    def retrieve_pdf_data(self, url):
        """
        Retrieve data from a PDF linked in a URL.

        Parameters:
        - url: URL of the PDF.

        Returns:
        - final_df: Pandas DataFrame containing data from the PDF.
        """
        response = requests.get(url)
        if response.status_code == 200:
            with open("downloaded_file.pdf", 'wb') as f:
                f.write(response.content)

            df_list = tabula.read_pdf(
                "downloaded_file.pdf", pages='all', multiple_tables=True)

            final_df = pd.concat(df_list)
            return final_df
        else:
            print(f"Failed to download the PDF from the URL:{url}")
            return pd.DataFrame()

    def upload_to_db(self, dataframes_and_tables):
        """
        Upload dataframes to the connected database.

        Parameters:
        - dataframes_and_tables: List of tuples containing dataframes and corresponding table names.
        """
        metadata = MetaData()
        metadata.reflect(bind=self.db_engine)

        for df, table_name in dataframes_and_tables:
            table = Table(table_name, metadata, autoload=True)
            data = df.to_dict(orient='records')
            with self.db_engine.connect() as connection:
                connection.execute(table.insert(), data)

    def some_other_method(self):
        """
        An example method demonstrating how to use the DataCleaning class and upload data to the database.
        """
        data_cleaner = data_cleaning.DataCleaning()
        dataframes_and_tables = [
            (data_cleaner.cleaned_data_df, 'dim_users'),
            (data_cleaner.clean_card_data_df, 'dim_card_details'),
            (data_cleaner.clean_stores_dataframe, 'dim_store_details'),
            (data_cleaner.clean_products_data, 'dim_products'),
            (data_cleaner.cleaned_orders_df, 'orders_table'),
            (data_cleaner.Clean_sales_date, 'dim_date_times')
        ]
        self.upload_to_db(dataframes_and_tables)

    @classmethod
    def from_yaml(cls, file_path=r"C:\Users\nacho\New folder\AiCore\multinational-retail-data-centralisation\db_creds.yml"):
        """
        Create an instance of DatabaseConnector from a YAML file.

        Parameters:
        - file_path: Path to the YAML file containing database credentials.

        Returns:
        - DatabaseConnector instance.
        """
        with open(file_path, 'r') as file:
            try:
                data = yaml.safe_load(file)
                return cls(
                    host=data['RDS_HOST'],
                    database=data['RDS_DATABASE'],
                    user=data['RDS_USER'],
                    password=data['RDS_PASSWORD'],
                    port=data['RDS_PORT']
                )
            except yaml.YAMLError as e:
                print(f"Error reading YAML file {file_path}: {e}")
                return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Retrieve and process data from a PDF linked in a URL.")
    parser.add_argument("url", type=str, help="URL of the PDF")
    args = parser.parse_args()

    db_connector = DatabaseConnector.from_yaml(
        r"C:\Users\nacho\New folder\AiCore\multinational-retail-data-centralisation\db_creds.yml")
    result_df = db_connector.retrieve_pdf_data(args.url)

    if not result_df.empty:
        print(result_df)
