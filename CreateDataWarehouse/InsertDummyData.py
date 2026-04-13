"""
Wealth Asset Management - Dummy Data Insertion Script
======================================================
This script inserts dummy data into the Microsoft Fabric Lakehouse
for the Wealth Asset Management project.

Usage:
    1. Update the connection details below (server, user, password, database)
    2. Set SCALE to control data volume:
       - "small"  : ~15K rows total  (quick testing)
       - "medium" : ~150K rows total (development)
       - "large"  : ~1.3M rows total (~1GB, production-like)
    3. Set CREATE_SCHEMA = True if tables/views/procedures don't exist yet
    4. Run: python InsertDummyData.py
"""

import pyodbc
from faker import Faker
import random
from datetime import datetime, timedelta
import time
import sys

# ============================================================
# CONFIGURATION - Update these before running
# ============================================================

# Database connection
SERVER = "your-server-name"
PORT = 1433
USER = "your-username"
PASSWORD = "your-password"
DATABASE = "your-database-name"

# Data scale: "small", "medium", or "large"
SCALE = "small"

# Set to True to create tables, views, and stored procedures first
CREATE_SCHEMA = False

# Batch size for commits (higher = faster but more memory)
BATCH_SIZE = 1000

# ============================================================
# SCALE CONFIGURATIONS
# ============================================================

SCALE_CONFIG = {
    "small": {
        "advisors": 100,
        "clients": 1000,
        "accounts": 2000,
        "assets": 1000,
        "transactions": 5000,
        "portfolios": 1000,
        "portfolio_assets": 3000,
        "projections": 2000,
    },
    "medium": {
        "advisors": 500,
        "clients": 10000,
        "accounts": 20000,
        "assets": 5000,
        "transactions": 50000,
        "portfolios": 10000,
        "portfolio_assets": 30000,
        "projections": 20000,
    },
    "large": {
        "advisors": 1000,
        "clients": 100000,
        "accounts": 200000,
        "assets": 10000,
        "transactions": 500000,
        "portfolios": 100000,
        "portfolio_assets": 300000,
        "projections": 200000,
    },
}

# ============================================================
# DOMAIN DATA - Realistic wealth management values
# ============================================================

RISK_PROFILES = ["High", "Medium", "Low"]
ACCOUNT_TYPES = ["Savings", "Checking", "Investment"]
ASSET_TYPES = ["Stock", "Bond", "Real Estate", "Commodity", "Cash"]
TRANSACTION_TYPES = ["Buy", "Sell", "Deposit", "Withdraw"]

PORTFOLIO_PREFIXES = [
    "Growth", "Conservative", "Balanced", "Aggressive", "Income",
    "Value", "Index", "Diversified", "Strategic", "Dynamic",
    "Global", "Emerging", "Retirement", "Legacy", "Core",
]

STOCK_NAMES = [
    "Apple Inc.", "Microsoft Corp", "Amazon.com", "Alphabet Inc.",
    "Tesla Inc.", "Meta Platforms", "NVIDIA Corp", "Berkshire Hathaway",
    "JPMorgan Chase", "Johnson & Johnson", "Visa Inc.", "Procter & Gamble",
    "UnitedHealth Group", "Home Depot", "Mastercard Inc.",
]

BOND_NAMES = [
    "US Treasury 10Y", "US Treasury 30Y", "Corporate AAA Bond",
    "Municipal Bond Fund", "High Yield Bond ETF", "Intl Govt Bond",
    "Inflation-Protected Securities", "Investment Grade Corp Bond",
]

REAL_ESTATE_NAMES = [
    "Vanguard Real Estate ETF", "Commercial Property Fund",
    "Residential REIT", "Industrial REIT", "Healthcare REIT",
    "Data Center REIT", "Retail Property Trust",
]

COMMODITY_NAMES = [
    "Gold Bullion", "Silver ETF", "Crude Oil Fund", "Natural Gas Trust",
    "Agricultural Commodity Fund", "Platinum Trust", "Copper ETF",
]

CASH_NAMES = [
    "Money Market Fund", "Certificate of Deposit", "High-Yield Savings",
    "Treasury Bills", "Short-Term Reserve Fund",
]


# ============================================================
# HELPER FUNCTIONS
# ============================================================

def get_connection():
    """Establish database connection."""
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={SERVER};"
        f"DATABASE={DATABASE};"
        f"UID={USER};"
        f"PWD={PASSWORD};"
    )
    return pyodbc.connect(conn_str)


def progress_bar(current, total, label="", bar_length=40):
    """Display a simple progress bar."""
    percent = current / total
    filled = int(bar_length * percent)
    bar = "█" * filled + "░" * (bar_length - filled)
    sys.stdout.write(f"\r  {label}: |{bar}| {current:,}/{total:,} ({percent:.0%})")
    sys.stdout.flush()
    if current == total:
        print()


def generate_asset_name(asset_type):
    """Generate a realistic asset name based on type."""
    name_map = {
        "Stock": STOCK_NAMES,
        "Bond": BOND_NAMES,
        "Real Estate": REAL_ESTATE_NAMES,
        "Commodity": COMMODITY_NAMES,
        "Cash": CASH_NAMES,
    }
    names = name_map.get(asset_type, STOCK_NAMES)
    base = random.choice(names)
    # Add variation so names aren't all duplicates
    suffix = random.choice(["", " Fund", " Trust", " ETF", " Class A", " Class B", ""])
    return f"{base}{suffix}"[:100]  # Respect NVARCHAR(100) limit


def generate_portfolio_name():
    """Generate a realistic portfolio name."""
    prefix = random.choice(PORTFOLIO_PREFIXES)
    suffix = random.choice(["Portfolio", "Fund", "Strategy", "Allocation", "Plan"])
    return f"{prefix} {suffix}"


def get_asset_value_range(asset_type):
    """Return realistic value ranges by asset type."""
    ranges = {
        "Stock": (10, 5000),
        "Bond": (500, 10000),
        "Real Estate": (5000, 500000),
        "Commodity": (50, 3000),
        "Cash": (1000, 100000),
    }
    return ranges.get(asset_type, (10, 1000))


# ============================================================
# SCHEMA CREATION
# ============================================================

def create_schema(cursor, db):
    """Create tables, views, and stored procedures."""

    print("\n📋 Creating schema...")

    # --- Tables ---
    table_sql = """
    -- Create Advisors table
    CREATE TABLE Advisors (
        AdvisorID INT IDENTITY(1,1) PRIMARY KEY,
        Name NVARCHAR(100) NOT NULL,
        ContactInfo NVARCHAR(100)
    );

    -- Create Clients table
    CREATE TABLE Clients (
        ClientID INT IDENTITY(1,1) PRIMARY KEY,
        Name NVARCHAR(100) NOT NULL,
        ContactInfo NVARCHAR(100),
        AdvisorID INT,
        RiskProfile NVARCHAR(50),
        FOREIGN KEY (AdvisorID) REFERENCES Advisors(AdvisorID)
    );

    -- Create Accounts table
    CREATE TABLE Accounts (
        AccountID INT IDENTITY(1,1) PRIMARY KEY,
        AccountType NVARCHAR(50) NOT NULL,
        ClientID INT,
        FOREIGN KEY (ClientID) REFERENCES Clients(ClientID)
    );

    -- Create Assets table
    CREATE TABLE Assets (
        AssetID INT IDENTITY(1,1) PRIMARY KEY,
        Name NVARCHAR(100) NOT NULL,
        AssetType NVARCHAR(50),
        CurrentValue DECIMAL(18, 2)
    );

    -- Create Transactions table
    CREATE TABLE Transactions (
        TransactionID INT IDENTITY(1,1) PRIMARY KEY,
        AccountID INT,
        AssetID INT,
        Date DATETIME,
        Type NVARCHAR(50),
        Amount DECIMAL(18, 2),
        FOREIGN KEY (AccountID) REFERENCES Accounts(AccountID),
        FOREIGN KEY (AssetID) REFERENCES Assets(AssetID)
    );

    -- Create Portfolios table
    CREATE TABLE Portfolios (
        PortfolioID INT IDENTITY(1,1) PRIMARY KEY,
        ClientID INT,
        Name NVARCHAR(100),
        RiskLevel NVARCHAR(50),
        FOREIGN KEY (ClientID) REFERENCES Clients(ClientID)
    );

    -- Create PortfolioAssets table
    CREATE TABLE PortfolioAssets (
        PortfolioAssetID INT IDENTITY(1,1) PRIMARY KEY,
        PortfolioID INT,
        AssetID INT,
        Allocation DECIMAL(18, 2),
        FOREIGN KEY (PortfolioID) REFERENCES Portfolios(PortfolioID),
        FOREIGN KEY (AssetID) REFERENCES Assets(AssetID)
    );

    -- Create Projections table
    CREATE TABLE Projections (
        ProjectionID INT IDENTITY(1,1) PRIMARY KEY,
        PortfolioID INT,
        FutureValue DECIMAL(18, 2),
        ProjectionDate DATETIME,
        FOREIGN KEY (PortfolioID) REFERENCES Portfolios(PortfolioID)
    );
    """

    # Execute each CREATE TABLE separately
    for statement in table_sql.split(";"):
        stmt = statement.strip()
        if stmt and "CREATE TABLE" in stmt:
            try:
                cursor.execute(stmt)
                db.commit()
                table_name = stmt.split("CREATE TABLE")[1].split("(")[0].strip()
                print(f"  ✅ Created table: {table_name}")
            except Exception as e:
                print(f"  ⚠️  Table may already exist: {e}")
                db.commit()

    # --- Views ---
    views = [
        ("PortfolioAssetAllocation", """
            CREATE VIEW PortfolioAssetAllocation AS
            SELECT p.PortfolioID, p.Name AS PortfolioName, pa.AssetID,
                   a.Name AS AssetName, pa.Allocation, a.CurrentValue,
                   (pa.Allocation / 100.0) * a.CurrentValue AS AllocatedValue
            FROM Portfolios p
            JOIN PortfolioAssets pa ON p.PortfolioID = pa.PortfolioID
            JOIN Assets a ON pa.AssetID = a.AssetID
        """),
        ("ClientPortfolioValue", """
            CREATE VIEW ClientPortfolioValue AS
            SELECT c.ClientID, c.Name AS ClientName,
                   SUM(pa.Allocation * a.CurrentValue / 100.0) AS TotalPortfolioValue
            FROM Clients c
            JOIN Portfolios p ON c.ClientID = p.ClientID
            JOIN PortfolioAssets pa ON p.PortfolioID = pa.PortfolioID
            JOIN Assets a ON pa.AssetID = a.AssetID
            GROUP BY c.ClientID, c.Name
        """),
        ("PortfolioSummary", """
            CREATE VIEW PortfolioSummary AS
            SELECT p.PortfolioID, p.Name AS PortfolioName,
                   c.ClientID, c.Name AS ClientName,
                   SUM(pa.Allocation * a.CurrentValue / 100.0) AS TotalPortfolioValue,
                   COUNT(DISTINCT pa.AssetID) AS NumberOfAssets
            FROM Portfolios p
            JOIN Clients c ON p.ClientID = c.ClientID
            JOIN PortfolioAssets pa ON p.PortfolioID = pa.PortfolioID
            JOIN Assets a ON pa.AssetID = a.AssetID
            GROUP BY p.PortfolioID, p.Name, c.ClientID, c.Name
        """),
        ("OverallWealthSummary", """
            CREATE VIEW OverallWealthSummary AS
            SELECT a.AssetType, COUNT(a.AssetID) AS NumberOfAssets,
                   SUM(a.CurrentValue) AS TotalWealth
            FROM Assets a
            GROUP BY a.AssetType
        """),
    ]

    for view_name, view_sql in views:
        try:
            cursor.execute(view_sql.strip())
            db.commit()
            print(f"  ✅ Created view: {view_name}")
        except Exception as e:
            print(f"  ⚠️  View {view_name} may already exist: {e}")
            db.commit()

    print("  Schema creation complete.\n")


# ============================================================
# DATA INSERTION FUNCTIONS
# ============================================================

def insert_advisors(cursor, db, count):
    """Insert advisor records."""
    print(f"\n👤 Inserting {count:,} Advisors...")
    fake = Faker()
    for i in range(1, count + 1):
        name = fake.name()
        contact_info = fake.phone_number()[:100]
        cursor.execute(
            "INSERT INTO Advisors (Name, ContactInfo) VALUES (?, ?)",
            (name, contact_info),
        )
        if i % BATCH_SIZE == 0:
            db.commit()
            progress_bar(i, count, "Advisors")
    db.commit()
    progress_bar(count, count, "Advisors")


def insert_clients(cursor, db, count, num_advisors):
    """Insert client records."""
    print(f"\n👥 Inserting {count:,} Clients...")
    fake = Faker()
    for i in range(1, count + 1):
        name = fake.name()
        contact_info = fake.phone_number()[:100]
        advisor_id = random.randint(1, num_advisors)
        risk_profile = random.choice(RISK_PROFILES)
        cursor.execute(
            "INSERT INTO Clients (Name, ContactInfo, AdvisorID, RiskProfile) VALUES (?, ?, ?, ?)",
            (name, contact_info, advisor_id, risk_profile),
        )
        if i % BATCH_SIZE == 0:
            db.commit()
            progress_bar(i, count, "Clients")
    db.commit()
    progress_bar(count, count, "Clients")


def insert_accounts(cursor, db, count, num_clients):
    """Insert account records."""
    print(f"\n🏦 Inserting {count:,} Accounts...")
    for i in range(1, count + 1):
        account_type = random.choice(ACCOUNT_TYPES)
        client_id = random.randint(1, num_clients)
        cursor.execute(
            "INSERT INTO Accounts (AccountType, ClientID) VALUES (?, ?)",
            (account_type, client_id),
        )
        if i % BATCH_SIZE == 0:
            db.commit()
            progress_bar(i, count, "Accounts")
    db.commit()
    progress_bar(count, count, "Accounts")


def insert_assets(cursor, db, count):
    """Insert asset records with realistic names and values."""
    print(f"\n📊 Inserting {count:,} Assets...")
    fake = Faker()
    for i in range(1, count + 1):
        asset_type = random.choice(ASSET_TYPES)
        # Mix between realistic names and Faker-generated company names
        if random.random() < 0.4:
            name = generate_asset_name(asset_type)
        else:
            name = fake.company()[:100]
        low, high = get_asset_value_range(asset_type)
        current_value = round(random.uniform(low, high), 2)
        cursor.execute(
            "INSERT INTO Assets (Name, AssetType, CurrentValue) VALUES (?, ?, ?)",
            (name, asset_type, current_value),
        )
        if i % BATCH_SIZE == 0:
            db.commit()
            progress_bar(i, count, "Assets")
    db.commit()
    progress_bar(count, count, "Assets")


def insert_portfolios(cursor, db, count, num_clients):
    """Insert portfolio records."""
    print(f"\n💼 Inserting {count:,} Portfolios...")
    for i in range(1, count + 1):
        client_id = random.randint(1, num_clients)
        name = generate_portfolio_name()
        risk_level = random.choice(RISK_PROFILES)
        cursor.execute(
            "INSERT INTO Portfolios (ClientID, Name, RiskLevel) VALUES (?, ?, ?)",
            (client_id, name, risk_level),
        )
        if i % BATCH_SIZE == 0:
            db.commit()
            progress_bar(i, count, "Portfolios")
    db.commit()
    progress_bar(count, count, "Portfolios")


def insert_portfolio_assets(cursor, db, count, num_portfolios, num_assets):
    """Insert portfolio-asset allocation records."""
    print(f"\n📈 Inserting {count:,} PortfolioAssets...")
    for i in range(1, count + 1):
        portfolio_id = random.randint(1, num_portfolios)
        asset_id = random.randint(1, num_assets)
        allocation = round(random.uniform(1, 100), 2)
        cursor.execute(
            "INSERT INTO PortfolioAssets (PortfolioID, AssetID, Allocation) VALUES (?, ?, ?)",
            (portfolio_id, asset_id, allocation),
        )
        if i % BATCH_SIZE == 0:
            db.commit()
            progress_bar(i, count, "PortfolioAssets")
    db.commit()
    progress_bar(count, count, "PortfolioAssets")


def insert_transactions(cursor, db, count, num_accounts, num_assets):
    """Insert transaction records spanning 4 years."""
    print(f"\n💳 Inserting {count:,} Transactions...")
    start_date = datetime(2020, 1, 1)
    for i in range(1, count + 1):
        account_id = random.randint(1, num_accounts)
        asset_id = random.randint(1, num_assets)
        date = start_date + timedelta(days=random.randint(1, 365 * 4))
        transaction_type = random.choice(TRANSACTION_TYPES)
        amount = round(random.uniform(100, 10000), 2)
        cursor.execute(
            "INSERT INTO Transactions (AccountID, AssetID, Date, Type, Amount) VALUES (?, ?, ?, ?, ?)",
            (account_id, asset_id, date, transaction_type, amount),
        )
        if i % BATCH_SIZE == 0:
            db.commit()
            progress_bar(i, count, "Transactions")
    db.commit()
    progress_bar(count, count, "Transactions")


def insert_projections(cursor, db, count, num_portfolios):
    """Insert projection records spanning 10 years into the future."""
    print(f"\n🔮 Inserting {count:,} Projections...")
    start_date = datetime(2024, 1, 1)
    for i in range(1, count + 1):
        portfolio_id = random.randint(1, num_portfolios)
        future_value = round(random.uniform(1000, 100000), 2)
        projection_date = start_date + timedelta(days=random.randint(1, 365 * 10))
        cursor.execute(
            "INSERT INTO Projections (PortfolioID, FutureValue, ProjectionDate) VALUES (?, ?, ?)",
            (portfolio_id, future_value, projection_date),
        )
        if i % BATCH_SIZE == 0:
            db.commit()
            progress_bar(i, count, "Projections")
    db.commit()
    progress_bar(count, count, "Projections")


# ============================================================
# MAIN EXECUTION
# ============================================================

def main():
    config = SCALE_CONFIG[SCALE]

    print("=" * 60)
    print("  Wealth Asset Management - Dummy Data Insertion")
    print("=" * 60)
    print(f"  Scale     : {SCALE}")
    print(f"  Server    : {SERVER}")
    print(f"  Database  : {DATABASE}")
    print(f"  Batch Size: {BATCH_SIZE:,}")
    print()

    total_rows = sum(config.values())
    print(f"  Total rows to insert: {total_rows:,}")
    print()
    print("  Table breakdown:")
    for table, count in config.items():
        print(f"    {table:20s} : {count:>10,}")
    print("=" * 60)

    # Connect
    print("\n🔌 Connecting to database...")
    db = get_connection()
    cursor = db.cursor()
    print("  ✅ Connected successfully!")

    start_time = time.time()

    try:
        # Optionally create schema
        if CREATE_SCHEMA:
            create_schema(cursor, db)

        # Insert data in foreign-key-safe order
        insert_advisors(cursor, db, config["advisors"])
        insert_clients(cursor, db, config["clients"], config["advisors"])
        insert_accounts(cursor, db, config["accounts"], config["clients"])
        insert_assets(cursor, db, config["assets"])
        insert_portfolios(cursor, db, config["portfolios"], config["clients"])
        insert_portfolio_assets(
            cursor, db, config["portfolio_assets"],
            config["portfolios"], config["assets"]
        )
        insert_transactions(
            cursor, db, config["transactions"],
            config["accounts"], config["assets"]
        )
        insert_projections(cursor, db, config["projections"], config["portfolios"])

        elapsed = time.time() - start_time
        print("\n" + "=" * 60)
        print(f"  ✅ All data inserted successfully!")
        print(f"  Total rows : {total_rows:,}")
        print(f"  Time taken : {elapsed:.1f} seconds ({elapsed/60:.1f} minutes)")
        print(f"  Rate       : {total_rows/elapsed:,.0f} rows/second")
        print("=" * 60)

    except Exception as e:
        print(f"\n❌ Error: {e}")
        db.rollback()
        raise
    finally:
        cursor.close()
        db.close()
        print("\n🔌 Connection closed.")


if __name__ == "__main__":
    main()
