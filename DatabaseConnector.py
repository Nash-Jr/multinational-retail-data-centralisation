import yaml
from sqlalchemy import create_engine, MetaData, Table
import requests
import tabula
import argparse
import pandas as pd
import data_cleaning


class DatabaseConnector:
    def __init__(self, host, database, user, password, port):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.port = port
        self.db_engine = self.init_db_engine()

    def init_db_engine(self):
        engine = create_engine(
            f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
        )
        return engine

    def read_db_creds(self, file_path):
        with open(file_path, 'r') as file:
            try:
                data = yaml.safe_load(file)
                print(data)  # Add this line to check the content of data
                return DatabaseConnector(
                    host=data['RDS_HOST'],
                    database=data['RDS_DATABASE'],
                    user=data['RDS_USER'],
                    password=data['RDS_PASSWORD'],
                    port=data['RDS_PORT']
                )
            except yaml.YAMLError as e:
                print(f"Error reading YAML file {file_path}: {e}")
                return None

    def list_db_tables(self):
        metadata = MetaData()
        metadata.reflect(bind=self.db_engine)
        table_names = metadata.tables.keys()
        return table_names

    def retrieve_pdf_data(self, url):
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
        metadata = MetaData()
        metadata.reflect(bind=self.db_engine)

        for df, table_name in dataframes_and_tables:
            table = Table(table_name, metadata, autoload=True)
            data = df.to_dict(orient='records')
            with self.db_engine.connect() as connection:
                connection.execute(table.insert(), data)

    def some_other_method(self):
        data_cleaner = data_cleaning.DataCleaning()
        dataframes_and_tables = [
            (data_cleaner.cleaned_data_df, 'dim_users'), (data_cleaner.clean_card_data_df, 'dim_card_details'), (data_cleaner.clean_stores_dataframe, 'dim_store_details'), (data_cleaner.clean_products_data, 'dim_products')]
        self.upload_to_db(dataframes_and_tables)

    @classmethod
    def from_yaml(cls, file_path='path/to/db_creds.yml'):
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

    # Create an instance of the DatabaseConnector class
    db_connector = DatabaseConnector.from_yaml(
        r"C:/Users/nacho/New folder/AiCore/multinational-retail-data-centralisation/.gitignore/db_creds.yml")

    # Use the db_connector instance for further operations
    result_df = db_connector.retrieve_pdf_data(args.url)

    if not result_df.empty:
        print(result_df)
