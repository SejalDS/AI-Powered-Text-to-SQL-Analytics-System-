import struct
from itertools import chain, repeat
import pyodbc
from .Credentials import Credentials

def get_connection():
    connection_string = f"Driver={{ODBC Driver 18 for SQL Server}};Server={Credentials.sql_endpoint},1433;Database={Credentials.database};Encrypt=Yes;TrustServerCertificate=No"
    token_as_bytes = bytes(Credentials.token, "UTF-8")
    encoded_bytes = bytes(chain.from_iterable(zip(token_as_bytes, repeat(0))))
    token_bytes = struct.pack("<i", len(encoded_bytes)) + encoded_bytes
    attrs_before = {1256: token_bytes}
    connection = pyodbc.connect(connection_string, attrs_before=attrs_before)
    return connection