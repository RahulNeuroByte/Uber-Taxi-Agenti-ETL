storage_account_name = ""
container_name = ""
# Paste your newly rotated storage key here.
storage_account_key = ""

# Paste your Microsoft Foundry API key here.
foundry_api_key = ""
foundry_endpoint = ""
foundry_model = ""

storage_base = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net"

source_path = f"{storage_base}/ncr_ride_bookings.csv"
base_path = f"{storage_base}/output"
bronze_path = f"{base_path}/bronze"
silver_path = f"{base_path}/silver"

gold_daily_path = f"{base_path}/gold/daily_trip_summary"
gold_location_path = f"{base_path}/gold/location_trip_summary"
gold_payment_path = f"{base_path}/gold/payment_type_trip_summary"

# "/Workspace/uber-taxi-agentic-etl/data"