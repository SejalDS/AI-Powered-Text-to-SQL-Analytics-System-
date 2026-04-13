import struct
from itertools import chain, repeat
import sqlalchemy as sa
from langchain import OpenAI, SQLDatabase
import urllib
import pyodbc
from langchain.chains import create_sql_query_chain
from langchain_openai import ChatOpenAI
from langchain_community.tools.sql_database.tool import QuerySQLDataBaseTool

# from azure.identity import AzureCliCredential
import os
os.environ["OPENAI_API_KEY"] = "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"
#
# credential = AzureCliCredential()

from azure.identity import InteractiveBrowserCredential

credential = InteractiveBrowserCredential()

sql_endpoint = "akshqqu6dk4u5bau762vux6pdy-2nwcokshbikelaeekw3pnvlhsy.datawarehouse.fabric.microsoft.com"

database = "wealth_data"

connection_string = f"Driver={{ODBC Driver 18 for SQL Server}};Server={sql_endpoint},1433;Database=f{database};Encrypt=Yes;TrustServerCertificate=No"

# This will remain as is
resource_url = "https://database.windows.net/.default"

# Get the token
token_object = credential.get_token(resource_url)

# print(token_object)
token = token_object.token

token_as_bytes = bytes(token, "UTF-8") # Convert the token to a UTF-8 byte string
encoded_bytes = bytes(chain.from_iterable(zip(token_as_bytes, repeat(0)))) # Encode the bytes to a Windows byte string
token_bytes = struct.pack("<i", len(encoded_bytes)) + encoded_bytes # Package the token into a bytes object
attrs_before = {1256: token_bytes}  # Attribute pointing to SQL_COPT_SS_ACCESS_TOKEN to pass access token to the driver

params = urllib.parse.quote(connection_string)
engine = sa.create_engine("mssql+pyodbc:///?odbc_connect={0}".format(params), connect_args={'attrs_before': attrs_before})
db = SQLDatabase(engine)

llm = ChatOpenAI(model="gpt-3.5-turbo", temperature=0, )
generate_query = create_sql_query_chain(llm, db)

query = generate_query.invoke({"question": "What are the top 3 clients with max portfolio value?"})
print(query)

execute_query = QuerySQLDataBaseTool(db=db)

print(execute_query.invoke(query))


