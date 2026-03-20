# testing big query connection

from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv
import os

load_dotenv()
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
credentials = service_account.Credentials.from_service_account_file(GOOGLE_APPLICATION_CREDENTIALS)

client = bigquery.Client(
    credentials=credentials,
    project=credentials.project_id
)

print("Project:", client.project)
datasets = list(client.list_datasets())
print("Datasets:", datasets)