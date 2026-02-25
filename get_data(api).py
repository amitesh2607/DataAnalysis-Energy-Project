from openelectricity import OEClient
from openelectricity.types import DataMetric, UnitFueltechType, UnitStatusType
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
import requests

# Load environment variables from .env file which contains the API key for Open Electricity
load_dotenv()
api_key = os.getenv("OPENELECTRICITY_API_KEY")


# Define the date range for the past week
end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
start_date = end_date - timedelta(days=7)

# The Direct URL for NEM data
url = "https://api.openelectricity.org.au/v4/data/network/NEM"

headers = {"Authorization": f"Bearer {api_key}"}

end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
start_date = end_date - timedelta(days=7)

load_dotenv()
api_key = os.getenv("OPENELECTRICITY_API_KEY")

# The Full  URL
url = f"https://api.openelectricity.org.au/v4/data/network/NEM?network_region=SA1&interval=1h&metrics=power&metrics=energy&primary_grouping=network_region&secondary_grouping=fueltech_group&date_start={start_date.isoformat()}&date_end={end_date.isoformat()}"

# Headers with API Key 
headers = {"Authorization": f"Bearer {api_key}"}

# One-Line Request
response = requests.get(url, headers=headers)

if response.status_code == 200:
    print("Success! Connection established.")
    
else:
    print(f"Error {response.status_code}: {response.text}")
