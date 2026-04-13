from azure.identity import DeviceCodeCredential

credential = DeviceCodeCredential()

resource_url = "https://database.windows.net/.default"
token_object = credential.get_token(resource_url)
print(token_object.token)