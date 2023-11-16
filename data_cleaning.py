import pandas as pd
import numpy as np


class DataCleaning:
    def clean_user_data(self, df):
        cleaned_data_df = df.dropna()
        cleaned_data_df['date_column'] = pd.to_datetime(
            cleaned_data_df['date_column'], errors='coerce')
        cleaned_data_df['numeric_column'] = pd.to_numeric(
            cleaned_data_df['numeric_column'], errors='coerce')
        return cleaned_data_df

    def clean_card_data(final_df):
        clean_card_data_df = final_df.dropna()
        clean_card_data_df['date_column'] = pd.to_datetime(
            clean_card_data_df['date_column'], errors='coerce')
        clean_card_data_df['numeric_column'] = pd.to_numeric(
            clean_card_data_df['numeric_column'], errors='coerce')
        return clean_card_data_df

    def clean_store_data(stores_dataframe):
        clean_stores_dataframe = stores_dataframe.dropna()
        clean_stores_dataframe['date_column'] = pd.to_datetime(
            clean_stores_dataframe['date_column'], errors='coerce')
        clean_stores_dataframe['numeric_column'] = pd.to_numeric(
            clean_stores_dataframe['numeric_column'], errors='coerce')
        return clean_stores_dataframe

    def convert_product_weights(extract_df):
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

    def clean_products_data(extract_df):
        clean_products_data = extract_df.dropna()
        clean_products_data['date_column'] = pd.to_datetime(
            clean_products_data['date_column'], errors='coerce')
        clean_products_data['numeric_column'] = pd.to_numeric(
            clean_products_data['numeric_column'], errors='coerce')
        return clean_products_data

    def clean_orders_data(df):
        columns_to_remove = ['first_name', 'last_name', '1']
        cleaned_orders_df = df.drop(columns=columns_to_remove, errors='ignore')
        return cleaned_orders_df
