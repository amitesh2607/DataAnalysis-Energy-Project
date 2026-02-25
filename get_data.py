from openelectricity import OEClient
from openelectricity.types import DataMetric, UnitFueltechType, UnitStatusType
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os

# Load environment variables from .env file which contains the API key for Open Electricity
load_dotenv()
api_key = os.getenv("OPENELECTRICITY_API_KEY")


# Define the date range for the past week
end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
start_date = end_date - timedelta(days=7)


client = OEClient(api_key=api_key)

with OEClient() as client:

    # Get network data for NEM
    response = client.get_network_data(
        network_code="NEM",
        metrics=[DataMetric.POWER, DataMetric.ENERGY],
        interval="1h",
        date_start=start_date,
        date_end=end_date,
        primary_grouping="network_region",
        secondary_grouping="fueltech_group",
    )

    df = response.to_pandas()
    print("\n--- NEM Energy Data (First 5 Rows) ---")
    print(df.head())

    df.to_csv("nem_energy_data.csv", index=False)
    print("\nData saved to nem_energy_data.csv")

    if "fueltech_group" in df.columns:
        print("\nEnergy Sources found in SA:")
        print(df["fueltech_group"].unique())

    
