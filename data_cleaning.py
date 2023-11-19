import pandas as pd
import numpy as np
import re


class DataCleaning:
    """
    Class for cleaning and processing data.

    Methods:
    - clean_user_data: Clean user data in a DataFrame.
    - clean_card_data: Clean card data in a DataFrame.
    - clean_store_data: Clean store data in a DataFrame.
    - convert_product_weights: Convert product weights in a DataFrame.
    - clean_products_data: Clean products data in a DataFrame.
    - clean_orders_data: Clean orders data in a DataFrame.
    - clean_sales_date: Clean sales date data in a DataFrame.
    """

    def clean_user_data(self, df):
        """
        Clean user data in a DataFrame.

        Parameters:
        - df: Pandas DataFrame containing user data.

        Returns:
        - cleaned_data_df: Cleaned Pandas DataFrame.
        """
        cleaned_data_df = df.dropna()
        cleaned_data_df['date_of_birth'] = pd.to_datetime(
            cleaned_data_df['date_of_birth'], errors='coerce')
        cleaned_data_df['join_date'] = pd.to_datetime(
            cleaned_data_df['join_date'], errors='coerce')
        cleaned_data_df['phone_number'] = cleaned_data_df['phone_number'].apply(
            lambda x: re.sub(r'\D', '', str(x)) if pd.notnull(x) else x)
        cleaned_data_df['country'] = cleaned_data_df['country'].str.strip(
        ).str.upper()
        return cleaned_data_df

    def clean_card_data(self, final_df):
        """
        Clean card data in a DataFrame.

        Parameters:
        - final_df: Pandas DataFrame containing card data.

        Returns:
        - clean_card_data_df: Cleaned Pandas DataFrame.
        """
        clean_card_data_df = final_df.dropna()
        clean_card_data_df['date_payment_confirmed'] = pd.to_datetime(
            clean_card_data_df['date_payment_confirmed'], errors='coerce')
        clean_card_data_df['expiry_date'] = pd.to_numeric(
            clean_card_data_df['expiry_date'], errors='coerce')
        clean_card_data_df['card_number'] = pd.to_numeric(
            clean_card_data_df['card_number'], errors='coerce')
        return clean_card_data_df

    def clean_store_data(self, stores_dataframe):
        """
        Clean store data in a DataFrame.

        Parameters:
        - stores_dataframe: Pandas DataFrame containing store data.

        Returns:
        - clean_stores_dataframe: Cleaned Pandas DataFrame.
        """
        clean_stores_dataframe = stores_dataframe.dropna()
        clean_stores_dataframe['date_column'] = pd.to_datetime(
            clean_stores_dataframe['date_column'], errors='coerce')
        clean_stores_dataframe['numeric_column'] = pd.to_numeric(
            clean_stores_dataframe['numeric_column'], errors='coerce')
        return clean_stores_dataframe

    def convert_product_weights(self, extract_df):
        """
        Convert product weights in a DataFrame.

        Parameters:
        - extract_df: Pandas DataFrame containing product data.

        Returns:
        - extract_df: Pandas DataFrame with converted product weights.
        """
        extract_df[['weight_value', 'weight_unit']
                   ] = extract_df['weight'].str.split(n=1, expand=True)
        extract_df['weight_value'] = pd.to_numeric(
            extract_df['weight_value'], errors='coerce')
        extract_df['weight_value'] = np.where(extract_df['weight_unit'].str.contains(
            'g'), extract_df['weight_value'] / 1000, extract_df['weight_value'])
        extract_df['weight_value'] = np.where(extract_df['weight_unit'].str.contains(
            'ml'), extract_df['weight_value'] / 1000, extract_df['weight_value'])
        extract_df.drop(columns=['weight_unit'], inplace=True)
        extract_df['weight_value'] = extract_df['weight_value'].astype(float)
        return extract_df

    def clean_products_data(self, extract_df):
        """
        Clean products data in a DataFrame.

        Parameters:
        - extract_df: Pandas DataFrame containing product data.

        Returns:
        - clean_products_data: Cleaned Pandas DataFrame.
        """
        clean_products_data = extract_df.dropna()
        clean_products_data['date_added'] = pd.to_datetime(
            clean_products_data['date_added'], errors='coerce')
        return clean_products_data

    def clean_orders_data(self, df):
        """
        Clean orders data in a DataFrame.

        Parameters:
        - df: Pandas DataFrame containing orders data.

        Returns:
        - cleaned_orders_df: Cleaned Pandas DataFrame.
        """
        columns_to_remove = ['first_name', 'last_name', '1']
        cleaned_orders_df = df.drop(columns=columns_to_remove, errors='ignore')
        return cleaned_orders_df

    def clean_sales_date(self, sales_date_df):
        """
        Clean sales date data in a DataFrame.

        Parameters:
        - sales_date_df: Pandas DataFrame containing sales date data.

        Returns:
        - Clean_sales_date: Cleaned Pandas DataFrame.
        """
        Clean_sales_date = sales_date_df.dropna()
        Clean_sales_date['timestamp'] = pd.to_datetime(
            Clean_sales_date['timestamp'], errors='coerce')
        Clean_sales_date['month'] = pd.to_numeric(
            Clean_sales_date['month'], errors='coerce')
        Clean_sales_date['year'] = pd.to_numeric(
            Clean_sales_date['year'], errors='coerce')
        Clean_sales_date['day'] = pd.to_numeric(
            Clean_sales_date['day'], errors='coerce')
        Clean_sales_date['time_period'] = Clean_sales_date['time_period'].astype(
            'category')
        time_period_mapping = {'morning': 'AM', 'afternoon': 'PM'}
        Clean_sales_date['time_period'] = Clean_sales_date['time_period'].map(
            time_period_mapping)


class DataCleaning:
    """
    Class for cleaning and processing data.

    Methods:
    - clean_user_data: Clean user data in a DataFrame.
    - clean_card_data: Clean card data in a DataFrame.
    - clean_store_data: Clean store data in a DataFrame.
    - convert_product_weights: Convert product weights in a DataFrame.
    - clean_products_data: Clean products data in a DataFrame.
    - clean_orders_data: Clean orders data in a DataFrame.
    - clean_sales_date: Clean sales date data in a DataFrame.
    """

    def clean_user_data(self, df):
        """
        Clean user data in a DataFrame.

        Parameters:
        - df: Pandas DataFrame containing user data.

        Returns:
        - cleaned_data_df: Cleaned Pandas DataFrame.
        """
        cleaned_data_df = df.dropna()
        cleaned_data_df['date_of_birth'] = pd.to_datetime(
            cleaned_data_df['date_of_birth'], errors='coerce')
        cleaned_data_df['join_date'] = pd.to_datetime(
            cleaned_data_df['join_date'], errors='coerce')
        cleaned_data_df['phone_number'] = cleaned_data_df['phone_number'].apply(
            lambda x: re.sub(r'\D', '', str(x)) if pd.notnull(x) else x)
        cleaned_data_df['country'] = cleaned_data_df['country'].str.strip(
        ).str.upper()
        return cleaned_data_df

    def clean_card_data(self, final_df):
        """
        Clean card data in a DataFrame.

        Parameters:
        - final_df: Pandas DataFrame containing card data.

        Returns:
        - clean_card_data_df: Cleaned Pandas DataFrame.
        """
        clean_card_data_df = final_df.dropna()
        clean_card_data_df['date_payment_confirmed'] = pd.to_datetime(
            clean_card_data_df['date_payment_confirmed'], errors='coerce')
        clean_card_data_df['expiry_date'] = pd.to_numeric(
            clean_card_data_df['expiry_date'], errors='coerce')
        clean_card_data_df['card_number'] = pd.to_numeric(
            clean_card_data_df['card_number'], errors='coerce')
        return clean_card_data_df

    def clean_store_data(self, stores_dataframe):
        """
        Clean store data in a DataFrame.

        Parameters:
        - stores_dataframe: Pandas DataFrame containing store data.

        Returns:
        - clean_stores_dataframe: Cleaned Pandas DataFrame.
        """
        clean_stores_dataframe = stores_dataframe.dropna()
        clean_stores_dataframe['date_column'] = pd.to_datetime(
            clean_stores_dataframe['date_column'], errors='coerce')
        clean_stores_dataframe['numeric_column'] = pd.to_numeric(
            clean_stores_dataframe['numeric_column'], errors='coerce')
        return clean_stores_dataframe

    def convert_product_weights(self, extract_df):
        """
        Convert product weights in a DataFrame.

        Parameters:
        - extract_df: Pandas DataFrame containing product data.

        Returns:
        - extract_df: Pandas DataFrame with converted product weights.
        """
        extract_df[['weight_value', 'weight_unit']
                   ] = extract_df['weight'].str.split(n=1, expand=True)
        extract_df['weight_value'] = pd.to_numeric(
            extract_df['weight_value'], errors='coerce')
        extract_df['weight_value'] = np.where(extract_df['weight_unit'].str.contains(
            'g'), extract_df['weight_value'] / 1000, extract_df['weight_value'])
        extract_df['weight_value'] = np.where(extract_df['weight_unit'].str.contains(
            'ml'), extract_df['weight_value'] / 1000, extract_df['weight_value'])
        extract_df.drop(columns=['weight_unit'], inplace=True)
        extract_df['weight_value'] = extract_df['weight_value'].astype(float)
        return extract_df

    def clean_products_data(self, extract_df):
        """
        Clean products data in a DataFrame.

        Parameters:
        - extract_df: Pandas DataFrame containing product data.

        Returns:
        - clean_products_data: Cleaned Pandas DataFrame.
        """
        clean_products_data = extract_df.dropna()
        clean_products_data['date_added'] = pd.to_datetime(
            clean_products_data['date_added'], errors='coerce')
        return clean_products_data

    def clean_orders_data(self, df):
        """
        Clean orders data in a DataFrame.

        Parameters:
        - df: Pandas DataFrame containing orders data.

        Returns:
        - cleaned_orders_df: Cleaned Pandas DataFrame.
        """
        columns_to_remove = ['first_name', 'last_name', '1']
        cleaned_orders_df = df.drop(columns=columns_to_remove, errors='ignore')
        return cleaned_orders_df

    def clean_sales_date(self, sales_date_df):
        """
        Clean sales date data in a DataFrame.

        Parameters:
        - sales_date_df: Pandas DataFrame containing sales date data.

        Returns:
        - Clean_sales_date: Cleaned Pandas DataFrame.
        """
        Clean_sales_date = sales_date_df.dropna()
        Clean_sales_date['timestamp'] = pd.to_datetime(
            Clean_sales_date['timestamp'], errors='coerce')
        Clean_sales_date['month'] = pd.to_numeric(
            Clean_sales_date['month'], errors='coerce')
        Clean_sales_date['year'] = pd.to_numeric(
            Clean_sales_date['year'], errors='coerce')
        Clean_sales_date['day'] = pd.to_numeric(
            Clean_sales_date['day'], errors='coerce')
        Clean_sales_date['time_period'] = Clean_sales_date['time_period'].astype(
            'category')
        time_period_mapping = {'morning': 'AM', 'afternoon': 'PM'}
        Clean_sales_date['time_period'] = Clean_sales_date['time_period'].map(
            time_period_mapping)
        uuid_pattern = re.compile(
            r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$')
        Clean_sales_date['is_valid_uuid'] = Clean_sales_date['date_uuid'].apply(
            lambda x: bool(uuid_pattern.match(str(x))))
        return Clean_sales_date
