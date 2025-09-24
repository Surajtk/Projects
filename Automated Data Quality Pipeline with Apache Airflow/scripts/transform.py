import pandas as pd
import os

def run():

    input_path = '/opt/airflow/data/silver/yellow_trip_cleaned.csv'

    df = pd.read_csv(input_path)

    # Parse datetime columns
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])

    # Average fare per passenger count
    summary = df.groupby('passenger_count').agg({
        'fare_amount': 'mean',
        'tip_amount': 'mean',
        'trip_distance': 'mean'
    }).reset_index()

    os.makedirs('/opt/airflow/data/gold', exist_ok=True)

    output_path = '/opt/airflow/data/gold/trip_summary.csv'

    summary.to_csv(output_path, index=False)
    print(f"Gold layer summary saved to {output_path}")
