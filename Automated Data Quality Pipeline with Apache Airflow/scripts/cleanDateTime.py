import pandas as pd

def run():
    print("Parsing datetime columns")
    
    df = pd.read_csv('/Users/surajtk/lakehouse_project/data/silver/clean_data2.csv')

    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'], errors='coerce')
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'], errors='coerce')

    df.to_csv('/Users/surajtk/lakehouse_project/data/silver/clean_data3.csv', index=False)
    print("Saved to clean_data3.csv")

