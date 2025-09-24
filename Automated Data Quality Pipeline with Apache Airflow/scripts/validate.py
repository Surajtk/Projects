import pandas as pd
import os

def run():
    print("Running validation checks")

    # Load the cleaned Silver dataset
    df = pd.read_csv('/opt/airflow/data/silver/yellow_trip_cleaned.csv')

    # Create a list to store validation results
    results = []

    # Rule 1: No negative fares
    invalid_fares = df[df['fare_amount'] < 0]
    results.append({
        'rule': 'Fare amount must be >= 0',
        'violations': len(invalid_fares)
    })

    # Rule 2: Valid passenger count (1–6)
    invalid_passengers = df[(df['passenger_count'] < 1) | (df['passenger_count'] > 6)]
    results.append({
        'rule': 'Passenger count must be between 1 and 6',
        'violations': len(invalid_passengers)
    })

    # Rule 3: Dropoff must be after pickup 
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    time_errors = df[df['tpep_dropoff_datetime'] < df['tpep_pickup_datetime']]
    results.append({
        'rule': 'Dropoff must be after pickup time',
        'violations': len(time_errors)
    })

    # Saving the report
    report_df = pd.DataFrame(results)
    report_df.to_csv(output_path, index=False)

    print("Validation complete. Report saved to:", '/opt/airflow/data/silver/validation_report.csv')
