import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
import requests
import json
from sqlalchemy import create_engine, text

# Load environment variables from .env file which contains the API key for Open Electricity
load_dotenv()
api_key = os.getenv("OPENELECTRICITY_API_KEY")


# Define the date range for the past week
end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
start_date = end_date - timedelta(weeks=7)

# The Direct URL for NEM data
url = "https://api.openelectricity.org.au/v4/data/network/NEM"

headers = {"Authorization": f"Bearer {api_key}"}


# The Full  URL
url = f"https://api.openelectricity.org.au/v4/market/network/NEM?interval=5m&metrics=price&metrics=demand&primary_grouping=network_region&secondary_grouping=fueltech&date_start={start_date.isoformat()}&date_end={end_date.isoformat()}"

# Headers with API Key
headers = {"Authorization": f"Bearer {api_key}"}

# One-Line Request
response = requests.get(url, headers=headers)

if response.status_code == 200:
    print("Success! Connection established.")

else:
    print(f"Error {response.status_code}: {response.text}")

json_data = response.json()
print(json.dumps(json_data, indent=2))

extracted_rows = []

# The Block Loop (e.g., The power block or the energy block)
for block in json_data["data"]:
    metric_name = block["metric"]

    # The result loop (This goes through each Fuel Type like Solar, Wind)
    for result in block["results"]:

        fuel_type = result["columns"].get("fueltech_group", "Unknown")
        region = result["columns"].get("region", "Unknown")

        # The Observation Loop (The timestamps and numbers)
        for observation in result["data"]:
            row = {
                "timestamp": observation[0],
                "region": region,
                "fuel_type": fuel_type,
                "metric": metric_name,
                "value": observation[1],
            }
            extracted_rows.append(row)


df = pd.DataFrame(extracted_rows)
df["timestamp"] = pd.to_datetime(df["timestamp"])

df_pivoted = df.pivot_table(
    index=["timestamp", "region", "fuel_type"],
    columns="metric",
    values="value",
).reset_index()

df_pivoted.columns.name = None

print("\n--- NEM Energy Data (First 5 Rows) ---")
print(df_pivoted.head())

print("\n--- DataFrame Summary ---")
print(df_pivoted.info())


df_pivoted.to_csv("nem_energy_data.csv", index=False)
print("\nData saved to nem_energy_data.csv")



mysql_password = os.getenv("MYSQL_PASSWORD")
engine = create_engine(f"mysql+pymysql://root:{mysql_password}@localhost/sapn_grid")
df_pivoted.to_sql("nem_market", con=engine, if_exists="replace", index=False)

print("Data saved to MySQL — sapn_grid.nem_market")
