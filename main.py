import os
import pandas as pd
import requests
import io
from datetime import date, timedelta, datetime
import time

class ETLClient:
    def __init__(self, base_url, api_key):
        self.base_url = base_url
        self.api_key = api_key

    def get_data(self, endpoint, day):
        while True:
            if endpoint.lower() == 'wind':
                url = f"{self.base_url}/{day}/renewables/{endpoint}gen.csv"
            else:
                url = f"{self.base_url}/{day}/renewables/{endpoint}gen.json"
            params = {'api_key': self.api_key}
            try:
                response = requests.get(url, params=params)
                response.raise_for_status()
            except requests.exceptions.HTTPError as err:
                if response.status_code == 429:
                    time.sleep(0.1)  # Wait for 100 ms
                    continue
                else:
                    raise err
            if endpoint.lower() == 'wind':
                data = pd.read_csv(io.StringIO(response.text))
            else:
                data = pd.DataFrame(response.json())

            # Set 'last_modified_utc' column to current UTC timestamp
            data['last_modified_utc'] = datetime.now()

            return data

    def clean_data(self, df):
        # Strip leading/trailing spaces from column names and convert to lowercase
        df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')

        # Reset index
        df = df.reset_index(drop=True)

        # Convert timestamps to datetime
        if 'naive_timestamp' in df.columns:
            df['naive_timestamp'] = pd.to_datetime(df['naive_timestamp'])

        return df

    def extract_latest_week_data(self, endpoint):
        data = []
        for i in range(7):
            day = (date.today() - timedelta(days=i)).strftime('%Y-%m-%d')
            data_day = self.get_data(endpoint, day)
            if isinstance(data_day, list):  # JSON data
                data.extend(data_day)
            else:  # DataFrame
                data.append(data_day)
        df = pd.concat(data, ignore_index=True) if isinstance(data[0], pd.DataFrame) else pd.DataFrame(data)

        # Clean the data
        cleaned_data = self.clean_data(df)

        # Drop duplicate columns
        cleaned_data = cleaned_data.loc[:,~cleaned_data.columns.duplicated()]

        # Convert 'last_modified_utc' to string format
        if 'last_modified_utc' in cleaned_data.columns:
            cleaned_data['last_modified_utc'] = cleaned_data['last_modified_utc'].astype(str)

        # Save the data to the output directory
        output_dir = './output'
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # Generate file name with range of days
        start_day = (date.today() - timedelta(days=6)).strftime('%Y-%m-%d')
        end_day = date.today().strftime('%Y-%m-%d')
        file_name = f'{endpoint}_data_{start_day}_to_{end_day}'

        # Check if file already exists, if so, append a timestamp
        if os.path.exists(f'{output_dir}/{file_name}.parquet') or os.path.exists(f'{output_dir}/{file_name}.csv'):
            timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
            file_name = f'{file_name}_{timestamp}'

        # Save as Parquet
        cleaned_data.to_parquet(f'{output_dir}/{file_name}.parquet')

        # Save as CSV
        cleaned_data.to_csv(f'{output_dir}/{file_name}.csv', index=False)

        return cleaned_data
    
    
def main():
    base_url = "http://localhost:8000"
    api_key = "ADU8S67Ddy!d7f?"
    client = ETLClient(base_url, api_key)
    solar_data = client.extract_latest_week_data("solar")
    wind_data = client.extract_latest_week_data("wind")

if __name__ == "__main__":
    main()