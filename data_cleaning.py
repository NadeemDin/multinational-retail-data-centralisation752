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


if __name__ == "__main__":
    cleaner = DataCleaning()
    cleaned_data = cleaner.clean_data()

    print(f"Modified DataFrame:")
    print(cleaned_data[['address', 'phone_number']])

    num_rows_pattern = cleaned_data.shape[0]
    print("--------")
    print(f"Number of rows matching pattern: {num_rows_pattern}")
    cleaned_data.to_excel('output_final.xlsx', index=True)

    