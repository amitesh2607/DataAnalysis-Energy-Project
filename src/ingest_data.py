import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os


df = pd.read_csv("../data/SouthAustralia_generation.csv")

# Clean csv file up before loading
df.columns = (
    df.columns.str.strip()
    .str.lower()
    .str.replace(" ", "_")
    .str.replace(r"[^a-z0-9_]", "", regex=True)
    .str.replace("__", "_")
)
df["date"] = pd.to_datetime(df["date"])

# Load environment variables from .env file which contains the MySQL password
load_dotenv()
mysql_password = os.getenv("MYSQL_PASSWORD")

# Save the cleaned DataFrame to MySQL
engine = create_engine(f"mysql+pymysql://root:{mysql_password}@localhost/sapn_grid")
df.to_sql("nem_all", con=engine, if_exists="replace", index=False)
