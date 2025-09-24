import pandas as pd

def run():
    print("Cleaning invalid values")
    df = pd.read_csv('/Users/surajtk/lakehouse_project/data/silver/clean_data1.csv')

    df = df[df['trip_distance'] > 0]
    df = df[df['passenger_count'] > 0]
    df = df[df['fare_amount'] >= 0]


    df.to_csv('/Users/surajtk/lakehouse_project/data/silver/clean_data2.csv', index=False)
    print("Saved to clean_data2.csv")

