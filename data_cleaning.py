'''
clean data
- assign data types 
- null values 
- duplicate values 
- 'NULL' entries
- clean phone numbers 
- clean clean clean weirdo values 
'''

import pandas as pd
import re
from data_extraction import DataExtractor
from datetime import datetime, time

class DataCleaning:
    def __init__(self):
        self.extractor = DataExtractor()
        self.pattern = r'\b(?=.*[A-Z])(?=.*\d)[A-Z\d]{10}\b'
        self.isd_code_map = {"UK": "+44", "US": "+1", "DE": "+49"}
        self.country_code_mapping = {
            'United Kingdom': 'UK',
            'United States': 'US',
            'Germany': 'DE',
            # Add more entries as needed
        }

    def clean_data(self):
        ext_df = self.extractor.read_rds_table()

        matching_entries = self._apply_pattern_matching(ext_df)
        matching_rows_pattern = matching_entries.any(axis=1)

        ext_df = self._remove_matching_rows(ext_df, matching_rows_pattern)
        ext_df = self._remove_null_rows(ext_df)
        ext_df = self._title_case_names(ext_df)
        ext_df = self._convert_to_datetime(ext_df)
        ext_df = self._update_country_code(ext_df)
        ext_df = self._drop_index_column(ext_df)
        ext_df = self._clean_phone_number(ext_df)
        ext_df = self._clean_address(ext_df)

        return ext_df

    def _apply_pattern_matching(self, df):
        return df.apply(self._contains_pattern)

    def _contains_pattern(self, series):
        return series.apply(lambda x: bool(re.search(self.pattern, str(x))))

    def _remove_matching_rows(self, df, matching_rows):
        return df[~matching_rows]

    def _remove_null_rows(self, df):
        return df[~df.apply(lambda row: 'NULL' in str(row), axis=1)]

    def _title_case_names(self, df):
        df['first_name'] = df['first_name'].str.title()
        df['last_name'] = df['last_name'].str.title()
        return df

    def _convert_to_datetime(self, df):
        try:
            df['date_of_birth'] = pd.to_datetime(df['date_of_birth']).dt.date
            df['join_date'] = pd.to_datetime(df['join_date']).dt.date
        except pd.errors.OutOfBoundsDatetime:
            print("Error: Out of bounds datetime encountered. Check date columns for anomalies.")
        return df

    def _update_country_code(self, df):
        df['country_code'] = df['country'].map(self.country_code_mapping)
        return df

    def _drop_index_column(self, df):
        return df.drop('index', axis=1)

    def _clean_phone_number(self, df):
        df['phone_number'] = df.apply(self._correct_phone_number, axis=1)
        return df

    def _correct_phone_number(self, row):
        result = re.sub("[^A-Za-z\d\+]", "", str(row['phone_number']))

        if not result.startswith(self.isd_code_map[row['country_code']]):
            result = self.isd_code_map[row['country_code']] + result

        if result.startswith(self.isd_code_map[row['country_code']] + "0"):
            result = result.replace(self.isd_code_map[row['country_code']] + "0", self.isd_code_map[row['country_code']])

        return result

    def _clean_address(self, df):
        df['address'] = df['address'].apply(self._clean_address_text)
        return df

    def _clean_address_text(self, address):
        address = re.sub(r'([.,])([^ ])', r'\1 \2', address)
        address = re.sub(r'[.,]', '', address)
        address = address.replace('\n', ' ')
        return address
    
    def clean_card_data(self, pdf_data):
        try:
            # Remove NULL rows
            cleaned_df = self._remove_null_rows(pdf_data)

            # Apply pattern matching and remove matching rows e.g. '1M38DYQTZV'
            matching_entries = self._apply_pattern_matching(cleaned_df)
            matching_rows_pattern = matching_entries.any(axis=1)
            cleaned_df = self._remove_matching_rows(cleaned_df, matching_rows_pattern)

            # Convert date_payment_confirmed column to datetime
            cleaned_df['date_payment_confirmed'] = pd.to_datetime(cleaned_df['date_payment_confirmed']).dt.date

            # Convert expiry_date column to datetime
            # Expiry column to have the MM-YY format
            cleaned_df['expiry_date'] = pd.to_datetime(cleaned_df['expiry_date'], format='%m/%y')
            cleaned_df['expiry_date'] = cleaned_df['expiry_date'].dt.strftime('%Y-%m')

                 
          
            return cleaned_df
        except Exception as e:
            print(f"Error cleaning card data: {e}")
            return None

    def clean_store_data(self,stores_data_df):
        try:
            # Remove NULL rows
            clean_store_df = self._remove_null_rows(stores_data_df)

            # Apply pattern matching and remove matching rows e.g. '1M38DYQTZV'
            matching_entries = self._apply_pattern_matching(clean_store_df)
            matching_rows_pattern = matching_entries.any(axis=1)
            clean_store_df = self._remove_matching_rows(clean_store_df, matching_rows_pattern)

            # Drop the 'lat' and 'index' columns
            clean_store_df = clean_store_df.drop(columns=['lat', 'index'], errors='ignore')

            # Map country codes to continents
            continent_mapping = {'GB': 'Europe', 'US': 'America', 'DE': 'Europe'}
            clean_store_df['continent'] = clean_store_df['country_code'].map(continent_mapping)

            # Clean address/staff numbers
            clean_store_df['address'] = clean_store_df['address'].apply(lambda address: re.sub(r'([.,])([^ ])', r'\1 \2', re.sub(r'[.,]', '', address.replace('\n', ' '))))
            clean_store_df['staff_numbers'] = clean_store_df['staff_numbers'].apply(lambda staff_numbers: re.sub(r'[a-zA-Z]', '', str(staff_numbers)))

            # Reset the index to the default pandas-assigned index
            clean_store_df = clean_store_df.reset_index(drop=True).rename_axis('index')

            # Datatype assignment 
            clean_store_df['opening_date'] = pd.to_datetime(clean_store_df['opening_date']).dt.date
            clean_store_df['latitude'] = clean_store_df['latitude'].astype(float).round(5)
            clean_store_df['longitude'] = clean_store_df['longitude'].astype(float).round(5)
            clean_store_df['staff_numbers'] = clean_store_df['staff_numbers'].astype(int)
                  
           
            return clean_store_df
        except Exception as e:
            print(f"Error cleaning card data: {e}")
            return None

    def clean_prod_data(self, products_df):
        try:
            # Apply pattern matching and remove matching rows e.g. '1M38DYQTZV'
            matching_entries = self._apply_pattern_matching(products_df)
            matching_rows_pattern = matching_entries.any(axis=1)
            products_df = self._remove_matching_rows(products_df, matching_rows_pattern)

            # Drop column 'Unnamed: 0' and set Python index as true
            products_df = products_df.drop(columns=['Unnamed: 0'], errors='ignore').reset_index(drop=True)

            # Rename the 'product_price' column to 'product_price_GBP'
            products_df.rename(columns={'product_price': 'product_price_GBP'}, inplace=True)

            # Remove the pound symbol (£) from 'product_price_GBP' column
            products_df['product_price_GBP'] = products_df['product_price_GBP'].str.replace('£', '').astype(float).round(2)

            # Convert the 'product_price_GBP' column to numeric values
            products_df['product_price_GBP'] = pd.to_numeric(products_df['product_price_GBP'], errors='coerce')

            # Capitalize all alphabetic characters in the 'product_code' column
            products_df['product_code'] = products_df['product_code'].str.upper()

            # Remove rows that are entirely empty
            products_df = products_df.dropna()

            # Convert date_ column to datetime
            products_df['date_added'] = pd.to_datetime(products_df['date_added']).dt.date

            # Convert the 'weight' column to a standardized format in kilograms
            def convert_to_kilos(weight):
                if 'kg' in weight.lower():  # Check if 'kg' is already present
                    parts = re.findall(r'\d+\.?\d*', weight)  # Extract numbers, allowing decimals
                    return float(parts[0])  # No conversion needed, return the value as is
                elif 'x' in weight:  # If the value is like '40 x 100g'
                    parts = re.findall(r'\d+', weight)  # Extract numbers using expression
                    total_weight = int(parts[0]) * int(parts[1])
                    return total_weight / 1000  # Convert to kilograms
                else:
                    parts = re.findall(r'\d+\.?\d*', weight)  # Extract numbers, allowing decimals
                    return float(parts[0]) / 1000  # Convert to kilograms
                
            # Convert the 'weight' column to 'weight_kg'
            products_df['weight'] = products_df['weight'].apply(convert_to_kilos).astype(float)
            products_df.rename(columns={'weight': 'weight_kg'}, inplace=True)

            # Reset the index to the default pandas-assigned index
            products_df = products_df.reset_index(drop=True).rename_axis('index')

            # Save to Excel
            products_df.to_excel('prods.xlsx', index=True)

            return products_df
        except Exception as e:
            print(f"Error cleaning product data: {e}")
            return None

    def clean_order_data(self, orders_df):
        try:
            # Apply pattern matching and remove matching rows e.g. '1M38DYQTZV'
            matching_entries = self._apply_pattern_matching(orders_df)
            matching_rows_pattern = matching_entries.any(axis=1)
            orders_df = self._remove_matching_rows(orders_df, matching_rows_pattern)

            # Remove rows that are entirely empty
            #orders_df = orders_df.dropna()

            # Capitalize all alphabetic characters in the 'product_code' column
            orders_df['product_code'] = orders_df['product_code'].str.upper()
            orders_df['store_code'] = orders_df['store_code'].str.upper()


            # Column Dropping
            columns_to_drop = ['1', 'index', 'level_0', 'first_name', 'last_name']

            # Drop the specified columns if they exist in the DataFrame
            orders_df = orders_df.drop(columns=columns_to_drop, errors='ignore')  

            # Reset the index to the default pandas-assigned index
            orders_df = orders_df.reset_index(drop=True).rename_axis('index')

            #product_quantity as int
            orders_df['product_quantity'] = orders_df['product_quantity'].astype(int)

            return orders_df
        except Exception as e:
            print(f"Error cleaning order data: {e}")
            return None



    def clean_json_data(self, json_df):
        try:
            # Apply pattern matching and remove matching rows e.g. '1M38DYQTZV'
            matching_entries = self._apply_pattern_matching(json_df)
            matching_rows_pattern = matching_entries.any(axis=1)
            json_df = self._remove_matching_rows(json_df, matching_rows_pattern)

            # Remove rows that are entirely empty
            json_df = self._remove_null_rows(json_df)

            # convert type to integer
            json_df['month'] = json_df['month'].astype(int)
            json_df['year'] = json_df['year'].astype(int)
            json_df['day'] = json_df['day'].astype(int)

            # create date colmn
            json_df['date'] = pd.to_datetime(json_df[['year', 'month', 'day']], errors='coerce').dt.date

            # Convert 'timestamp' column to datetime.time
            json_df['timestamp'] = json_df['timestamp'].apply(lambda x: datetime.strptime(x, "%H:%M:%S").time())

            # Combine 'date' and 'timestamp' columns into a single 'datetime' column
            json_df['datetime'] = json_df.apply(lambda row: datetime.combine(row['date'], row['timestamp']), axis=1)

            json_df = json_df.reset_index(drop=True).rename_axis('index')



            
     
          
            return json_df
        except Exception as e:
            print(f"Error cleaning json data: {e}")
            return None

'''if __name__ == "__main__":
    cleaner = DataCleaning()
    cleaned_data = cleaner.clean_data()

    print(f"Modified DataFrame:")
    print(cleaned_data[['address', 'phone_number']])

    num_rows_pattern = cleaned_data.shape[0]
    print("--------")
    print(f"Number of rows matching pattern: {num_rows_pattern}")
    cleaned_data.to_excel('output_final.xlsx', index=True)'''

    