import datetime
import os
import logging
from azure.data.tables import TableServiceClient

def main(event):
    logging.info("EVENT GRID FUNCTION INVOKED")

    storage_account = os.environ["STORAGE_ACCOUNT_NAME"]
    storage_key = os.environ["AZURE_STORAGE_KEY"]

    table_service = TableServiceClient(
        endpoint=f"https://{storage_account}.table.core.windows.net/",
        credential=storage_key
    )

    table_client = table_service.get_table_client("eventlogs")

    for e in event:
        entity = {
            "PartitionKey": "AzureEvents",
            "RowKey": str(datetime.datetime.utcnow().timestamp()),
            "eventTime": e.get("eventTime", "NA"),
            "eventType": e.get("eventType", "NA"),
            "subject": e.get("subject", "NA"),
            "region": "eastasia"
        }

        table_client.create_entity(entity)

