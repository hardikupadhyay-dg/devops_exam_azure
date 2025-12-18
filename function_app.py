import json
import logging
import uuid
import os
from datetime import datetime

import azure.functions as func
from azure.data.tables import TableServiceClient

app = func.FunctionApp()

@app.function_name(name="EventGridHttpHandler")
@app.route(route="eventgrid", auth_level=func.AuthLevel.FUNCTION)
def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.warning("🔥 EVENT GRID FUNCTION INVOKED 🔥")

    try:
        events = req.get_json()
    except Exception as e:
        logging.error(f"JSON error: {e}")
        return func.HttpResponse("Bad request", status_code=400)

    # ✅ REQUIRED: Event Grid validation handshake
    if isinstance(events, list) and events[0].get("eventType") == "Microsoft.EventGrid.SubscriptionValidationEvent":
        validation_code = events[0]["data"]["validationCode"]
        logging.warning("✅ Event Grid validation handshake received")
        return func.HttpResponse(
            json.dumps({"validationResponse": validation_code}),
            mimetype="application/json",
            status_code=200
        )

    # ✅ Storage connection
    account_name = os.environ["STORAGE_ACCOUNT_NAME"]
    account_key = os.environ["STORAGE_ACCOUNT_KEY"]

    service = TableServiceClient(
        endpoint=f"https://{account_name}.table.core.windows.net",
        credential=account_key
    )
    table = service.get_table_client("eventlogs")

    for event in events:
	try:
        	entity = {
            	"PartitionKey": "eventgrid",
            	"RowKey": str(uuid.uuid4()),
            	"eventType": event.get("eventType"),
            	"subject": event.get("subject"),
            	"eventTime": event.get("eventTime"),
            	"topic": event.get("topic")	
        	}
        	table.create_entity(entity)
        	logging.warning(f"📝 Event written: {entity['eventType']}")
	except Exception as e:
		logging.error(f"❌ Table write failed: {str(e)}")
    return func.HttpResponse("OK", status_code=200)

