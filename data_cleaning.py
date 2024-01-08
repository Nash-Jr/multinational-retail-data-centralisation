import pandas as pd
import numpy as np
import re
from dateutil.parser import parse
import usaddress
import uuid


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

    def datetime_conversion(self, value):
        """
        Convert date string to a standardized format.

        Parameters:
        - value: Date string.

        Returns:
        - str: Standardized date string (DD-MM-YYYY) or 'NULL' for invalid dates.
        """
        # List of possible date formats
        date_formats = ['%Y-%m-%d', '%Y %B %d', '%Y/%m/%d', '%B %Y %d']

        for format in date_formats:
            try:
                # Attempt to parse the date using different formats
                parsed_date = parse(
                    value, fuzzy_with_tokens=True, dayfirst=True, yearfirst=False)
                return parsed_date[0].strftime('%d-%m-%Y')
            except (ValueError, StopIteration):
                pass

        return 'NULL'

    def clean_user_data(self, df):
        cleaned_data_df = df.copy()
        cleaned_data_df = cleaned_data_df.drop(columns=['index'])

        # Handle 'date_of_birth' column
        cleaned_data_df['date_of_birth'] = cleaned_data_df['date_of_birth'].apply(
            self.datetime_conversion)

        # Handle 'NULL' values
        cleaned_data_df['date_of_birth'] = np.where(
            cleaned_data_df['date_of_birth'] == 'NULL',
            np.nan,
            cleaned_data_df['date_of_birth']
        )

        # Remove rows with non-date values in 'date_of_birth'
        cleaned_data_df = cleaned_data_df.dropna(subset=['date_of_birth'])

        for col in cleaned_data_df.columns:
            if pd.api.types.is_numeric_dtype(cleaned_data_df[col]):
                # Handle numeric columns (e.g., phone numbers)
                cleaned_data_df[col] = cleaned_data_df[col].apply(lambda x: re.sub(
                    r'\D', '', str(x)) if pd.notnull(x) else x)
            elif pd.api.types.is_string_dtype(cleaned_data_df[col]):
                # Handle string columns (e.g., country names)
                cleaned_data_df[col] = cleaned_data_df[col].str.strip(
                ).str.upper()

        # Print unique values in 'date_of_birth' column
        print(cleaned_data_df)

        return cleaned_data_df

    def clean_card_data(self, card_df):
        """
        Clean card data in a DataFrame.

        Parameters:
        - final_df: Pandas DataFrame containing card data.

        Returns:
        - clean_card_data_df: Cleaned Pandas DataFrame.
        """
        clean_card_data_df = card_df.dropna()
        clean_card_data_df['date_payment_confirmed'] = pd.to_datetime(
            clean_card_data_df['date_payment_confirmed'], errors='coerce')
        clean_card_data_df['expiry_date'] = pd.to_datetime(
            clean_card_data_df['expiry_date'], errors='coerce', format='%m/%y')
        clean_card_data_df['expiry_date'] = clean_card_data_df['expiry_date'].dt.strftime(
            '%m/%y')

        return clean_card_data_df

    def clean_store_data(self, stores_data):
        """
        Clean store data in a DataFrame.

        Parameters:
        - stores_df: Pandas DataFrame containing store data.

        Returns:
        - clean_stores_dataframe: Cleaned Pandas DataFrame.
        """
        # Drop rows where all values are missing
        stores_df = stores_data.dropna(how='all')

        # Drop columns with a threshold of at least 1 non-null value
        stores_df = stores_df.dropna(thresh=1, axis=1)

        # Additional cleaning steps
        stores_df['address'] = stores_df['address'].str.replace("\n", " ")
        stores_df['continent'] = stores_df['continent'].apply(
            lambda x: x.replace("ee", ""))
        stores_df = stores_df.dropna(subset=['address'])

        valid_country_codes = ['US', 'GB', 'DE']
        stores_df = stores_df[stores_df['country_code'].isin(
            valid_country_codes)]

        print(stores_df)
        return stores_df

    def clean_products_data(self, extract_df):
        """
        Clean products data in a DataFrame.

        Parameters:
        - extract_df: Pandas DataFrame containing product data.

        Returns:
        - clean_products_data: Cleaned Pandas DataFrame.
        """
        clean_products_data = extract_df.copy()
        clean_products_data = clean_products_data.drop(columns=['Unnamed: 0'])
        clean_products_data = clean_products_data.dropna()

        # Extract numeric values and units from the 'weight' column
        numeric_values = clean_products_data['weight'].str.extract('(\d+)')
        units = clean_products_data['weight'].str.extract('([a-zA-Z]+)')

        # Convert numeric values to numeric type and handle missing values
        numeric_values = pd.to_numeric(numeric_values[0], errors='coerce')

        # Divide 'weight' values by 1000 only when the unit is not 'kg'
        clean_products_data['weight'] = np.where(
            units[0] != 'kg', numeric_values / 1000, numeric_values)

        # Remove non-numeric characters from 'product_price'
        clean_products_data['product_price'] = clean_products_data['product_price'].replace(
            '[^\d.]', '', regex=True)

        # Convert 'product_price' to numeric
        clean_products_data['product_price'] = pd.to_numeric(
            clean_products_data['product_price'], errors='coerce')

        # Clean 'date_added' column
        clean_products_data['date_added'] = pd.to_datetime(
            clean_products_data['date_added'], errors='coerce')

        # Clean 'uuid' column
        def convert_to_uuid(x):
            try:
                return str(uuid.UUID(x)) if pd.notnull(x) else x
            except ValueError:
                return None

        clean_products_data['uuid'] = clean_products_data['uuid'].apply(
            convert_to_uuid)
        print(clean_products_data)
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
        cleaned_orders_df = df.drop(columns=columns_to_remove).copy()

        # Drop rows if all columns have missing values
        cleaned_orders_df = cleaned_orders_df.dropna(how='all')

        # for col in cleaned_orders_df.columns:
        #     if pd.api.types.is_numeric_dtype(cleaned_orders_df[col]):
        #         # Handle missing values and remove non-numeric characters
        #         cleaned_orders_df[col] = cleaned_orders_df[col].apply(
        #             lambda x: re.sub(r'\D', '', str(x)) if pd.notnull(x) else x
        #         )
        #     elif pd.api.types.is_datetime64_any_dtype(cleaned_orders_df[col]):
        #         # Convert to datetime and handle missing values
        #         cleaned_orders_df[col] = pd.to_datetime(
        #             cleaned_orders_df[col], errors='coerce')
        #     elif pd.api.types.is_string_dtype(cleaned_orders_df[col]):
        #         # Strip whitespace and convert to uppercase
        #         cleaned_orders_df[col] = cleaned_orders_df[col].str.strip(
        #         ).str.upper()

        # Drop specified columns and reset index
        cleaned_orders_df = cleaned_orders_df.drop(
            columns=['level_0', 'index']).reset_index(drop=True)

        print("Null values after cleaning:\n",
              cleaned_orders_df.isnull().sum())

        # Example logging
        print(f"Number of rows before cleaning: {len(df)}")
        # Perform cleaning steps
        print(f"Number of rows after cleaning: {len(cleaned_orders_df)}")
        print(cleaned_orders_df)

        return cleaned_orders_df

    def clean_sales_date(self, sales_date_df):
        """
        Clean sales date data in a DataFrame.

        Parameters:
        - sales_date_df: Pandas DataFrame containing sales date data.

        Returns:
        - clean_sales_date: Cleaned Pandas DataFrame.
        """
        clean_sales_date = sales_date_df.copy()
        clean_sales_date.replace('NULL', np.nan, inplace=True)

        # Drop rows if all specified columns have missing values
        # Specify columns to check for missing values
        columns_to_dropna = ['year', 'day']
        clean_sales_date = clean_sales_date.dropna(
            subset=columns_to_dropna, how='all')

        # Convert 'year' and 'day' columns to numeric, handling missing values
        clean_sales_date['year'] = pd.to_numeric(
            clean_sales_date['year'], errors='coerce')
        clean_sales_date['day'] = pd.to_numeric(
            clean_sales_date['day'], errors='coerce')

        print(clean_sales_date)
        return clean_sales_date
