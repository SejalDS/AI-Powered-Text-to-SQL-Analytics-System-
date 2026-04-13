import struct
from itertools import chain, repeat
import pyodbc

# Retrive the Token Value for your login
from azure.identity import InteractiveBrowserCredential
credential = InteractiveBrowserCredential()

# SQL End point from LangchainFabrics Lakehouse
sql_endpoint = "akshqqu6dk4u5bau762vux6pdy-2nwcokshbikelaeekw3pnvlhsy.datawarehouse.fabric.microsoft.com"

# Name of the database
database = "wealth_data"

# Make connection string
connection_string = f"Driver={{ODBC Driver 18 for SQL Server}};Server={sql_endpoint},1433;Database=f{database};Encrypt=Yes;TrustServerCertificate=No"

# This will remain as is
resource_url = "https://database.windows.net/.default"

# Get the token
token_object = credential.get_token(resource_url)
# print(token_object.token)

token = token_object.token
# Set Token Values
token_as_bytes = bytes(token, "UTF-8") # Convert the token to a UTF-8 byte string
encoded_bytes = bytes(chain.from_iterable(zip(token_as_bytes, repeat(0)))) # Encode the bytes to a Windows byte string
token_bytes = struct.pack("<i", len(encoded_bytes)) + encoded_bytes # Package the token into a bytes object
attrs_before = {1256: token_bytes}  # Attribute pointing to SQL_COPT_SS_ACCESS_TOKEN to pass access token to the driver

# Connect with PYODBC
connection = pyodbc.connect(connection_string, attrs_before=attrs_before)
cursor = connection.cursor()

# Test Query
cursor.execute("SELECT Top(3) * FROM [wealth_data].[dbo].[Accounts]")
rows = cursor.fetchall()
print(rows)

cursor.close()
connection.close()
