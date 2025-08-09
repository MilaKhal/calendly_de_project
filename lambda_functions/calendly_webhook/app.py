import json
import boto3
import uuid
import datetime
import traceback

s3 = boto3.client('s3')
BUCKET_NAME = 'de-calendly-project-bucket' 
ALLOWED_EVENT_TYPES = {
    "https://api.calendly.com/event_types/d639ecd3-8718-4068-955a-436b10d72c78",
    "https://api.calendly.com/event_types/dbb4ec50-38cd-4bcd-bbff-efb7b5a6f098",
    "https://api.calendly.com/event_types/bb339e98-7a67-4af2-b584-8dbf95564312"
}

def lambda_handler(event, context):
    try:
        body = event.get("body")
        if body is None:
            print("Missing body in event:", event)
            return {"statusCode": 400, "body": "Missing body"}

        data = json.loads(body)
        print("Parsed data:", data)

        event_type = data.get("payload", {}).get("scheduled_event", {}).get("event_type")
        print("Event type:", event_type)

        if event_type not in ALLOWED_EVENT_TYPES:
            print(f"Skipping event_type {event_type}")
            return {
                'statusCode': 200,
                'body': json.dumps({'message': 'Event type not of interest'})
            }

        target_folder = 'raw'
        today = datetime.datetime.utcnow().strftime('%Y-%m-%d')
        timestamp = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H-%M-%S')
        unique_id = uuid.uuid4()
        file_name = f"{target_folder}/{today}/{timestamp}_{unique_id}.json"

        s3.put_object(Bucket=BUCKET_NAME, Key=file_name, Body=json.dumps(data))
        print(f"Uploaded to S3: {file_name}")

        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Webhook received successfully"})
        }

    except Exception as e:
        print("Error occurred:", str(e))
        traceback.print_exc()
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
