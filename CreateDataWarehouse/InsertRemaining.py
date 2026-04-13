import pyodbc
from faker import Faker
import random
from datetime import datetime, timedelta
from azure.identity import InteractiveBrowserCredential
import struct

server = "akshqqu6dk4u5bau762vux6pdy-uq3lso5ka4iupjbfxhakqweq6i.datawarehouse.fabric.microsoft.com"
database = "asset-management"

print("Opening browser for login...")
credential = InteractiveBrowserCredential()
token = credential.get_token("https://database.windows.net/.default")
token_bytes = token.token.encode("UTF-16-LE")
token_struct = struct.pack(f'<I{len(token_bytes)}s', len(token_bytes), token_bytes)

db = pyodbc.connect(
    'DRIVER={ODBC Driver 18 for SQL Server};'
    'SERVER=' + server + ';'
    'DATABASE=' + database + ';'
    'Encrypt=yes;',
    attrs_before={1256: token_struct}
)

cursor = db.cursor()
fake = Faker()
print("Connected!\n")

# Insert remaining Transactions (4501 to 5000)
print("Inserting remaining Transactions...")
start_date = datetime(2020, 1, 1)
for i in range(4501, 5001):
    account_id = random.randint(1, 2000)
    asset_id = random.randint(1, 1000)
    date = start_date + timedelta(days=random.randint(1, 365 * 4))
    transaction_type = random.choice(['Buy', 'Sell', 'Deposit', 'Withdraw'])
    amount = round(random.uniform(100, 10000), 2)
    cursor.execute(
        "INSERT INTO Transactions (TransactionID, AccountID, AssetID, Date, Type, Amount) VALUES (?, ?, ?, ?, ?, ?)",
        (i, account_id, asset_id, date, transaction_type, amount)
    )
db.commit()
print("  Done: Remaining transactions inserted.\n")

# Insert all Projections
print("Inserting Projections...")
start_date = datetime(2024, 1, 1)
for i in range(1, 2001):
    portfolio_id = random.randint(1, 1000)
    future_value = round(random.uniform(1000, 100000), 2)
    projection_date = start_date + timedelta(days=random.randint(1, 365 * 10))
    cursor.execute(
        "INSERT INTO Projections (ProjectionID, PortfolioID, FutureValue, ProjectionDate) VALUES (?, ?, ?, ?)",
        (i, portfolio_id, future_value, projection_date)
    )
    if i % 500 == 0:
        db.commit()
        print(f"  Projections: {i}/2000")
db.commit()
print("  Done: 2000 projections inserted.\n")

cursor.close()
db.close()
print("=" * 50)
print("All remaining data inserted successfully!")
print("=" * 50)