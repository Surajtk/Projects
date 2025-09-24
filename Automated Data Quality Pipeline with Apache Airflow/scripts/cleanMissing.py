
import pandas as pd

def run():
    print("Cleaning missing values")

    df = pd.read_csv('/Users/surajtk/lakehouse_project/data/bronze/yellow_tripdata.csv')

    # Drop rows missing critical fields
    df = df.dropna(subset=[
        'tpep_pickup_datetime',
        'tpep_dropoff_datetime',
        'passenger_count',
        'trip_distance',
        'fare_amount'
    ])

    df.to_csv('/Users/surajtk/lakehouse_project/data/silver/clean_data1.csv', index=False)
    print("Saved to clean_data1.csv")
