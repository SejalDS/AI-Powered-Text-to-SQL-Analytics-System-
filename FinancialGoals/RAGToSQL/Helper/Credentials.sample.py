
class Credentials:

    # SQL End point from Fabric Data Warehouse
    sql_endpoint = "your-endpoint.datawarehouse.fabric.microsoft.com"
    # Name of the database
    database = "your_database_name"
    # This will remain as is
    resource_url = "https://database.windows.net/.default"
    # Azure token (run connection.py to generate, expires every ~1 hour)
    token = 'your-azure-token'
    # Open AI Key
    open_ai_key = "your-openai-api-key"
    # Model
    model = 'gpt-3.5-turbo-16k'
