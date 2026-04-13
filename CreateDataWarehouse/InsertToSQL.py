import pyodbc
from faker import Faker
import random
from datetime import datetime, timedelta
from azure.identity import DeviceCodeCredential
import struct

# ============================================================
# CONNECTION SETUP
# ============================================================
server = "akshqqu6dk4u5bau762vux6pdy-2nwcokshbikelaeekw3pnvlhsy.datawarehouse.fabric.microsoft.com"
database = "wealth_data_WH"

# Authenticate via device code
print("Authenticating...")
credential = DeviceCodeCredential()
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
print("Connected successfully!\n")

# ============================================================
# CONFIGURATION
# ============================================================
fake = Faker()

NUM_ADVISORS = 100
NUM_CLIENTS = 1000
NUM_ACCOUNTS = 2000
NUM_ASSETS = 1000
NUM_PORTFOLIOS = 1000
NUM_PORTFOLIO_ASSETS = 3000
NUM_TRANSACTIONS = 5000
NUM_PROJECTIONS = 2000

# ============================================================
# INSERT DATA
# ============================================================

# 1. Insert Advisors
print("Inserting Advisors...")
for i in range(1, NUM_ADVISORS + 1):
    name = fake.name()
    contact_info = fake.phone_number()
    cursor.execute(
        "INSERT INTO Advisors (AdvisorID, Name, ContactInfo) VALUES (?, ?, ?)",
        (i, name, contact_info)
    )
    if i % 100 == 0:
        print(f"  Advisors: {i}/{NUM_ADVISORS}")
db.commit()
print(f"  Done: {NUM_ADVISORS} advisors inserted.\n")

# 2. Insert Clients
print("Inserting Clients...")
for i in range(1, NUM_CLIENTS + 1):
    name = fake.name()
    contact_info = fake.phone_number()
    advisor_id = random.randint(1, NUM_ADVISORS)
    risk_profile = random.choice(['High', 'Medium', 'Low'])
    cursor.execute(
        "INSERT INTO Clients (ClientID, Name, ContactInfo, AdvisorID, RiskProfile) VALUES (?, ?, ?, ?, ?)",
        (i, name, contact_info, advisor_id, risk_profile)
    )
    if i % 200 == 0:
        db.commit()
        print(f"  Clients: {i}/{NUM_CLIENTS}")
db.commit()
print(f"  Done: {NUM_CLIENTS} clients inserted.\n")

# 3. Insert Accounts
print("Inserting Accounts...")
for i in range(1, NUM_ACCOUNTS + 1):
    account_type = random.choice(['Savings', 'Checking', 'Investment'])
    client_id = random.randint(1, NUM_CLIENTS)
    cursor.execute(
        "INSERT INTO Accounts (AccountID, AccountType, ClientID) VALUES (?, ?, ?)",
        (i, account_type, client_id)
    )
    if i % 500 == 0:
        db.commit()
        print(f"  Accounts: {i}/{NUM_ACCOUNTS}")
db.commit()
print(f"  Done: {NUM_ACCOUNTS} accounts inserted.\n")

# 4. Insert Assets
print("Inserting Assets...")
for i in range(1, NUM_ASSETS + 1):
    name = fake.company()
    asset_type = random.choice(['Stock', 'Bond', 'Real Estate', 'Commodity', 'Cash'])
    current_value = round(random.uniform(10, 1000), 2)
    cursor.execute(
        "INSERT INTO Assets (AssetID, Name, AssetType, CurrentValue) VALUES (?, ?, ?, ?)",
        (i, name, asset_type, current_value)
    )
    if i % 200 == 0:
        db.commit()
        print(f"  Assets: {i}/{NUM_ASSETS}")
db.commit()
print(f"  Done: {NUM_ASSETS} assets inserted.\n")

# 5. Insert Portfolios
print("Inserting Portfolios...")
for i in range(1, NUM_PORTFOLIOS + 1):
    client_id = random.randint(1, NUM_CLIENTS)
    name = f"Portfolio {fake.word()}"
    risk_level = random.choice(['High', 'Medium', 'Low'])
    cursor.execute(
        "INSERT INTO Portfolios (PortfolioID, ClientID, Name, RiskLevel) VALUES (?, ?, ?, ?)",
        (i, client_id, name, risk_level)
    )
    if i % 200 == 0:
        db.commit()
        print(f"  Portfolios: {i}/{NUM_PORTFOLIOS}")
db.commit()
print(f"  Done: {NUM_PORTFOLIOS} portfolios inserted.\n")

# 6. Insert PortfolioAssets
print("Inserting PortfolioAssets...")
for i in range(1, NUM_PORTFOLIO_ASSETS + 1):
    portfolio_id = random.randint(1, NUM_PORTFOLIOS)
    asset_id = random.randint(1, NUM_ASSETS)
    allocation = round(random.uniform(1, 100), 2)
    cursor.execute(
        "INSERT INTO PortfolioAssets (PortfolioAssetID, PortfolioID, AssetID, Allocation) VALUES (?, ?, ?, ?)",
        (i, portfolio_id, asset_id, allocation)
    )
    if i % 500 == 0:
        db.commit()
        print(f"  PortfolioAssets: {i}/{NUM_PORTFOLIO_ASSETS}")
db.commit()
print(f"  Done: {NUM_PORTFOLIO_ASSETS} portfolio assets inserted.\n")

# 7. Insert Transactions
print("Inserting Transactions...")
start_date = datetime(2020, 1, 1)
for i in range(1, NUM_TRANSACTIONS + 1):
    account_id = random.randint(1, NUM_ACCOUNTS)
    asset_id = random.randint(1, NUM_ASSETS)
    date = start_date + timedelta(days=random.randint(1, 365 * 4))
    transaction_type = random.choice(['Buy', 'Sell', 'Deposit', 'Withdraw'])
    amount = round(random.uniform(100, 10000), 2)
    cursor.execute(
        "INSERT INTO Transactions (TransactionID, AccountID, AssetID, Date, Type, Amount) VALUES (?, ?, ?, ?, ?, ?)",
        (i, account_id, asset_id, date, transaction_type, amount)
    )
    if i % 500 == 0:
        db.commit()
        print(f"  Transactions: {i}/{NUM_TRANSACTIONS}")
db.commit()
print(f"  Done: {NUM_TRANSACTIONS} transactions inserted.\n")

# 8. Insert Projections
print("Inserting Projections...")
start_date = datetime(2024, 1, 1)
for i in range(1, NUM_PROJECTIONS + 1):
    portfolio_id = random.randint(1, NUM_PORTFOLIOS)
    future_value = round(random.uniform(1000, 100000), 2)
    projection_date = start_date + timedelta(days=random.randint(1, 365 * 10))
    cursor.execute(
        "INSERT INTO Projections (ProjectionID, PortfolioID, FutureValue, ProjectionDate) VALUES (?, ?, ?, ?)",
        (i, portfolio_id, future_value, projection_date)
    )
    if i % 500 == 0:
        db.commit()
        print(f"  Projections: {i}/{NUM_PROJECTIONS}")
db.commit()
print(f"  Done: {NUM_PROJECTIONS} projections inserted.\n")

# ============================================================
# DONE
# ============================================================
cursor.close()
db.close()
print("=" * 50)
print("All data inserted successfully!")
print(f"Total rows: {NUM_ADVISORS + NUM_CLIENTS + NUM_ACCOUNTS + NUM_ASSETS + NUM_PORTFOLIOS + NUM_PORTFOLIO_ASSETS + NUM_TRANSACTIONS + NUM_PROJECTIONS:,}")
print("=" * 50)