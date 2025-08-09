import json
import boto3
import urllib.request
import re

s3 = boto3.client('s3')
bucket_name = 'de-calendly-project-bucket'

def lambda_handler(event, context):
    try:
        # Step 1: Fetch the file index JSON from the public S3 URL
        index_url = "https://dea-data-bucket.s3.us-east-1.amazonaws.com/calendly_spend_data/file_index.json"
        with urllib.request.urlopen(index_url) as response:
            if response.status != 200:
                raise Exception(f"Failed to fetch index: {response.status}")
            file_index = json.load(response)
            all_files = file_index.get("files", [])

        if not all_files:
            raise Exception("No files found in the index.")

        # Step 2: Get the most recent file (assumes filenames sortable by date in name)
        latest_file = sorted(all_files)[-1]

        # Extract date from filename, assuming pattern like 'spend_data_YYYY-MM-DD.json'
        match = re.search(r'(\d{4}-\d{2}-\d{2})', latest_file)
        if not match:
            raise Exception(f"Could not extract date from filename '{latest_file}'")
        file_date = match.group(1)

        data_url = f"https://dea-data-bucket.s3.us-east-1.amazonaws.com/calendly_spend_data/{latest_file}"

        with urllib.request.urlopen(data_url) as response:
            if response.status != 200:
                raise Exception(f"Failed to fetch data: {response.status}")
            spend_data = json.load(response)

        # Step 4: Filter records where the 'date' matches the file_date
        filtered_records = [record for record in spend_data if record.get("date") == file_date]

        if not filtered_records:
            raise Exception(f"No records found for date {file_date} in file {latest_file}")

        # Step 5: Save filtered data as line-delimited JSON to S3 under daily_spends/file_date=YYYY-MM-DD/
        s3_key_prefix = f'daily_spends/file_date={file_date}/'
        file_name = f'spend_data_{file_date}.json'
        s3_key = s3_key_prefix + file_name

        # Create line-delimited JSON string (one JSON object per line)
        line_delimited_json = '\n'.join(json.dumps(record) for record in filtered_records)

        s3.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=line_delimited_json.encode('utf-8'),
            ContentType='application/json'
        )

        return {
            'statusCode': 200,
            'body': json.dumps({'message': f'Filtered data saved to {s3_key}', 'records_count': len(filtered_records)})
        }

    except Exception as e:
        # Any exception causes the function to error out -> Lambda will retry automatically (max 3 tries)
        # If DLQ configured, event is sent there after retries fail
        raise e
