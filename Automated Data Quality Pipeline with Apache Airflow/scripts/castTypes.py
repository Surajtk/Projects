import pandas as pd

def run():
    print("Casting column data types")

    df = pd.read_csv('data/silver/clean_data3.csv')

    numeric_fields = [
        'passenger_count', 'trip_distance', 'fare_amount',
        'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
        'improvement_surcharge', 'total_amount',
        'congestion_surcharge', 'Airport_fee'
    ]

    for col in numeric_fields:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    df = df.dropna(subset=numeric_fields)  # optional: drop if type conversion fails

    df.to_csv('data/silver/yellow_trip_cleaned.csv', index=False)
    print("Final cleaned file ready for Gold layer")
