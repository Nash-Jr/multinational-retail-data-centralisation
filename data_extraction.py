import pandas as pd
from sqlalchemy import create_engine, MetaData, Table
import boto3
import requests
import tabula


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
        metadata = MetaData()
        metadata.reflect(bind=self.rds_engine)
        table_names = metadata.tables.keys()
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

    def retrieve_pdf_data(self, url):
        """
        Retrieve data from a PDF linked in a URL.

        Parameters:
        - url (str): URL of the PDF.

        Returns:
        - pd.DataFrame: Pandas DataFrame containing data from the PDF.
        """
        try:
            response = requests.get(url)
            response.raise_for_status()

            with open("downloaded_file.pdf", 'wb') as f:
                f.write(response.content)

            df_list = tabula.read_pdf(
                "downloaded_file.pdf", pages='all', multiple_tables=True)

            final_df = pd.concat(df_list, ignore_index=True)
            return final_df

        except requests.exceptions.RequestException as e:
            print(f"Failed to download the PDF from the URL: {url}")
            print(f"Error: {e}")
            return pd.DataFrame()

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
            print(extract_df)
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
                number_of_stores = response.json().get('number_stores')
                return number_of_stores
            else:
                print(
                    f"Failed to retrieve the number of stores. Status code: {response.status_code}")
                return None
        except Exception as e:
            print(f"An error occurred: {e}")
            return None

    def retrieve_stores_data(self, store_endpoint, header, number_of_stores):
        """
        Retrieve stores data from an API endpoint.

        Parameters:
        - number_of_stores_endpoint: API endpoint for retrieving the number of stores.
        - header: Header information for the API request.

        Returns:
        - stores_dataframe: Pandas DataFrame containing store information.
        """
        stores_data = []

        for store_number in range(1, number_of_stores + 1):
            store_url = store_endpoint.format(store_number=store_number)
            response = requests.get(store_url, headers=header)

            if response.status_code == 200:
                store_data = response.json()
                stores_data.append(store_data)
            else:
                print(
                    f"Failed to retrieve store data for store {store_number}. Status code: {response.status_code}")

        if stores_data:
            stores_df = pd.DataFrame(stores_data)
            print(stores_df)
            return stores_df
        else:
            print("Failed to retrieve store data from the API.")
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
                print(sales_date_df)
                return sales_date_df
            else:
                print(
                    f"Failed to retrieve data. Status code: {response.status_code}")
                return None
        except Exception as e:
            print(f"An error occurred: {e}")
            return None


if __name__ == "__main__":
    from DatabaseConnector import DatabaseConnector
    db_credentials = {
        'config_file_path': r"C:\Users\nacho\New folder\AiCore\multinational-retail-data-centralisation\db_creds.yml"
    }

    db_connector = DatabaseConnector(**db_credentials)
    data_extractor = DataExtractor(db_connector)

    # Provide the correct parameters for retrieve_stores_data
    number_of_stores_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
    headers = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
    number_of_stores = data_extractor.list_number_of_stores(
        number_of_stores_endpoint)

    pdf_url = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
    card_df = data_extractor.retrieve_pdf_data(pdf_url)

    # Display the combined DataFrame
    print(card_df)

    if number_of_stores is not None:
        result = data_extractor.retrieve_stores_data(
            'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}',
            headers,
            number_of_stores
        )
        print(result)
    else:
        print("Failed to retrieve the number of stores.")
