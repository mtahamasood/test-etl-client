
# ETL Client for Solar and Wind Data

This project implements an ETL (Extract, Transform, Load) client in Python3 that interacts with a sensor API data source to extract data from the Solar and Wind endpoints, transform the data, and load it into an output directory.

## Getting Started

1. Setup a virtual environment (`venv`, `conda`, etc.)
2. As per the instructions in the original Readme.md file , have that data source API Running
3. Install requirements into your environment: `pip install -r requirements.txt` 
4. It is being assumed that the data source API is running on localhost.  Later , we could get the exact server
and details from some config file

## Usage

Run the main script to start the ETL process:

```bash
python main.py
```

This will extract the latest week's data from the Solar and Wind endpoints, transform the naive timestamps to UTC, ensure proper column naming and types, and load the data into the `./output` directory. The data is saved in both Parquet and CSV formats.

## Design and Implementation

The ETL client is implemented as a Python class, `ETLClient`, with methods for each step of the ETL process:

- `get_data(endpoint, day)`: Extracts data from the specified endpoint for the specified day.
- `clean_data(df)`: Transforms the data by cleaning up column names, resetting the index, and converting timestamps to datetime.
- `extract_latest_week_data(endpoint)`: Extracts the latest week's data from the specified endpoint, cleans the data, and saves it to the output directory.

The main script creates an instance of `ETLClient` and calls `extract_latest_week_data` for both the Solar and Wind endpoints.

## Testing
Due to shortage of time , tests have not been written . However , they would be in below directions:

1) Unit test
2) Functional tests 

Would need to test in multiple ways e.g.:
1) Having same date range checked again for same sensor type and seeing if it creates new file with naming convention 
2) Testing the different methods of class in isolation
3) Stress testing ( even though API rate limiting has been caterer for in retries but other aspects can be stressed )
4) Boundary value testing of various kinds 
5) PyTest and other mock testing frameworks could also be used 

## License

This project is open source and available under the [MIT License](LICENSE).
```
