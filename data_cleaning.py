import pandas as pd
import numpy as np
import re
from dateutil.parser import parse
import usaddress


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
        stores_df = stores_data.drop(columns=['lat', 'index'])
        cleaned_store_data = stores_df[~stores_df.isnull().any(axis=1)]
        cleaned_store_data['address'] = cleaned_store_data['address'].str.replace(
            "\n", " ")
        cleaned_store_data['continent'] = cleaned_store_data['continent'].apply(
            lambda x: x.replace("ee", ""))
        cleaned_store_data = cleaned_store_data.dropna(subset=['address'])
        valid_country_codes = ['US', 'GB', 'DE']
        cleaned_store_data = cleaned_store_data[cleaned_store_data['country_code'].isin(
            valid_country_codes)]
        print(cleaned_store_data)
        return cleaned_store_data

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
        clean_products_data = clean_products_data[~clean_products_data.isnull().any(
            axis=1)]

        # Extract numeric values and units from the 'weight' column
        numeric_values = clean_products_data['weight'].str.extract('(\d+)')
        units = clean_products_data['weight'].str.extract('([a-zA-Z]+)')

        # Convert numeric values to numeric type and handle missing values
        numeric_values = pd.to_numeric(numeric_values[0], errors='coerce')

        # Divide 'weight' values by 1000 only when the unit is not 'kg'
        clean_products_data['weight'] = np.where(
            units[0] != 'kg', numeric_values / 1000, numeric_values)

        # Remove '£' symbol from the 'product_price' column and convert to numeric
        clean_products_data['product_price'] = clean_products_data['product_price'].str.replace(
            '£', '')
        clean_products_data['product_price'] = pd.to_numeric(
            clean_products_data['product_price'], errors='coerce')

        clean_products_data = clean_products_data.loc[clean_products_data['product_price'].notna(
        )]
        clean_products_data.loc[:, 'date_added'] = pd.to_datetime(
            clean_products_data['date_added'], errors='coerce')
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
        cleaned_orders_df = df.drop(
            columns=columns_to_remove).copy()

        for col in cleaned_orders_df.columns:
            if pd.api.types.is_numeric_dtype(cleaned_orders_df[col]):
                cleaned_orders_df[col] = cleaned_orders_df[col].apply(
                    lambda x: re.sub(r'\D', '', str(x)) if pd.notnull(x) else x
                )
            elif pd.api.types.is_datetime64_any_dtype(cleaned_orders_df[col]):
                cleaned_orders_df[col] = pd.to_datetime(
                    cleaned_orders_df[col], errors='coerce')
            elif pd.api.types.is_string_dtype(cleaned_orders_df[col]):
                cleaned_orders_df[col] = cleaned_orders_df[col].str.strip(
                ).str.upper()

        cleaned_orders_df = cleaned_orders_df.drop(
            columns=['level_0', 'index']).reset_index(drop=True)  # Drop specified columns and reset index

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
        clean_sales_date = clean_sales_date[~clean_sales_date.isnull().any(
            axis=1)]
        clean_sales_date['year'] = pd.to_numeric(
            clean_sales_date['year'], errors='coerce')
        clean_sales_date['day'] = pd.to_numeric(
            clean_sales_date['day'], errors='coerce')
        print(clean_sales_date)
        return clean_sales_date


# if __name__ == "__main__":
#    from data_extraction import DataExtractor
#
#    data_extractor = DataExtractor(db_connector)
#
#    # User Data
#    df = data_extractor.get_user_data()
#    cleaned_df = data_extractor.clean_user_data(df)
#    print("Cleaned User Data:")
#    print(cleaned_df)
#    print("User Data printed.")

    # Card Data
#    final_df = data_extractor.get_card_data()
#    cleaned_card_df = data_extractor.clean_card_data(final_df)
#    print("\nCleaned Card Data:")
#    print(cleaned_card_df)
#    print("Card Data printed.")

    # # Store Data
    # stores_dataframe = data_extractor.get_store_data()
    # cleaned_stores_df = data_extractor.clean_store_data(stores_dataframe)
    # print("\nCleaned Store Data:")
    # print(cleaned_stores_df)
    # print("Store Data printed.")

    # # Product Data
    # extract_df = data_extractor.get_product_data()
    # converted_extract_df = data_extractor.convert_product_weights(extract_df)
    # cleaned_products_df = data_extractor.clean_products_data(extract_df)
    # print("\nCleaned Products Data:")
    # print(cleaned_products_df)
    # print("Products Data printed.")

    # # Orders Data
    # orders_df = data_extractor.get_orders_data()
    # cleaned_orders_df = data_extractor.clean_orders_data(orders_df)
    # print("\nCleaned Orders Data:")
    # print(cleaned_orders_df)
    # print("Orders Data printed.")

    # # Sales Date Data
    # sales_date_df = data_extractor.get_sales_date_data()
    # cleaned_sales_date_df = data_extractor.clean_sales_date(sales_date_df)
    # print("\nCleaned Sales Date Data:")
    # print(cleaned_sales_date_df)
    # print("Sales Date Data printed.")
