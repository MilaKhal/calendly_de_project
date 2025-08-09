import sys
import boto3
import json
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import Row
from pyspark.sql.functions import lit, to_timestamp, col
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, BooleanType, ArrayType, StructType
)

# Initialize Spark and Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# AWS clients
s3 = boto3.client('s3')
athena = boto3.client('athena')

# Config
raw_bucket = "de-calendly-project-bucket"
raw_prefix = "raw/"
processed_prefix = "processed/events/"
athena_output = f"s3://{raw_bucket}/athena-results/"
athena_db = "calendly_project"
athena_table = "events_raw"

# Recursive flatten dict, preserve lists as-is
def flatten_dict(d, parent_key='', sep='_'):
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            items.append((new_key, v))
        else:
            items.append((new_key, v))
    return dict(items)

# Schema with timestamp fields as StringType initially
schema = StructType([
    StructField("cancel_url", StringType(), True),
    StructField("created_at", StringType(), True),  # STRING for now
    StructField("email", StringType(), True),
    StructField("event", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("invitee_scheduled_by", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("name", StringType(), True),
    StructField("new_invitee", StringType(), True),
    StructField("no_show", StringType(), True),
    StructField("old_invitee", StringType(), True),
    StructField("payment", StringType(), True),
    StructField("questions_and_answers", ArrayType(StructType([
        StructField("answer", StringType(), True),
        StructField("position", IntegerType(), True),
        StructField("question", StringType(), True)
    ])), True),
    StructField("reconfirmation", StringType(), True),
    StructField("reschedule_url", StringType(), True),
    StructField("rescheduled", BooleanType(), True),
    StructField("routing_form_submission", StringType(), True),
    StructField("scheduled_event_created_at", StringType(), True),
    StructField("scheduled_event_end_time", StringType(), True),
    StructField("scheduled_event_event_guests", ArrayType(StringType()), True),
    StructField("scheduled_event_event_memberships", ArrayType(StructType([
        StructField("user", StringType(), True),
        StructField("user_email", StringType(), True),
        StructField("user_name", StringType(), True)
    ])), True),
    StructField("scheduled_event_event_type", StringType(), True),
    StructField("scheduled_event_invitees_counter_total", IntegerType(), True),
    StructField("scheduled_event_invitees_counter_active", IntegerType(), True),
    StructField("scheduled_event_invitees_counter_limit", IntegerType(), True),
    StructField("scheduled_event_location_location", StringType(), True),
    StructField("scheduled_event_location_type", StringType(), True),
    StructField("scheduled_event_meeting_notes_html", StringType(), True),
    StructField("scheduled_event_meeting_notes_plain", StringType(), True),
    StructField("scheduled_event_name", StringType(), True),
    StructField("scheduled_event_start_time", StringType(), True),
    StructField("scheduled_event_status", StringType(), True),
    StructField("scheduled_event_updated_at", StringType(), True),
    StructField("scheduled_event_uri", StringType(), True),
    StructField("scheduling_method", StringType(), True),
    StructField("status", StringType(), True),
    StructField("text_reminder_number", StringType(), True),
    StructField("timezone", StringType(), True),
    StructField("tracking_utm_campaign", StringType(), True),
    StructField("tracking_utm_source", StringType(), True),
    StructField("tracking_utm_medium", StringType(), True),
    StructField("tracking_utm_content", StringType(), True),
    StructField("tracking_utm_term", StringType(), True),
    StructField("tracking_salesforce_uuid", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("uri", StringType(), True),

    StructField("event_date", StringType(), True)  # partition column
])

# Helper to extract nested fields flattened
def transform_flattened_row(row):
    flat = flatten_dict(row)

    se = row.get("scheduled_event", {})
    if se:
        flat["scheduled_event_created_at"] = se.get("created_at")
        flat["scheduled_event_end_time"] = se.get("end_time")
        flat["scheduled_event_event_guests"] = se.get("event_guests", [])
        flat["scheduled_event_event_memberships"] = se.get("event_memberships", [])
        flat["scheduled_event_event_type"] = se.get("event_type")
        ic = se.get("invitees_counter", {})
        flat["scheduled_event_invitees_counter_total"] = ic.get("total")
        flat["scheduled_event_invitees_counter_active"] = ic.get("active")
        flat["scheduled_event_invitees_counter_limit"] = ic.get("limit")
        loc = se.get("location", {})
        flat["scheduled_event_location_location"] = loc.get("location")
        flat["scheduled_event_location_type"] = loc.get("type")
        flat["scheduled_event_meeting_notes_html"] = se.get("meeting_notes_html")
        flat["scheduled_event_meeting_notes_plain"] = se.get("meeting_notes_plain")
        flat["scheduled_event_name"] = se.get("name")
        flat["scheduled_event_start_time"] = se.get("start_time")
        flat["scheduled_event_status"] = se.get("status")
        flat["scheduled_event_updated_at"] = se.get("updated_at")
        flat["scheduled_event_uri"] = se.get("uri")

    tracking = row.get("tracking", {})
    flat["tracking_utm_campaign"] = tracking.get("utm_campaign")
    flat["tracking_utm_source"] = tracking.get("utm_source")
    flat["tracking_utm_medium"] = tracking.get("utm_medium")
    flat["tracking_utm_content"] = tracking.get("utm_content")
    flat["tracking_utm_term"] = tracking.get("utm_term")
    flat["tracking_salesforce_uuid"] = tracking.get("salesforce_uuid")

    return flat


# List date folders under raw/
response = s3.list_objects_v2(Bucket=raw_bucket, Prefix=raw_prefix, Delimiter='/')
date_folders = [prefix['Prefix'] for prefix in response.get('CommonPrefixes', [])]

for folder_path in date_folders:
    date_part = folder_path.rstrip('/').split('/')[-1]  # get 'YYYY-MM-DD' from 'raw/YYYY-MM-DD/'
    print(f"📂 Processing folder: {folder_path}")

    # List JSON files in the folder
    response = s3.list_objects_v2(Bucket=raw_bucket, Prefix=folder_path)
    json_files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith(".json")]

    if not json_files:
        print(f"🚫 No JSON files found in {folder_path}, skipping.")
        continue

    # Read and transform all JSON files
    flattened_rows = []
    for key in json_files:
        file_obj = s3.get_object(Bucket=raw_bucket, Key=key)
        raw_content = file_obj['Body'].read().decode('utf-8')

        try:
            data = json.loads(raw_content)
            payload = data.get("payload", {})
            transformed = transform_flattened_row(payload)
            flattened_rows.append(transformed)
        except Exception as e:
            print(f"⚠️ Failed to parse {key}: {e}")

    if not flattened_rows:
        print(f"🚫 No valid events found in {folder_path}, skipping.")
        continue

    # Create DataFrame with schema (timestamps as strings)
    df = spark.createDataFrame(flattened_rows, schema=schema)

    # Add partition column
    df = df.withColumn("event_date", lit(date_part))

    # Convert timestamp strings to actual TimestampType
    timestamp_cols = [
        "created_at",
        "scheduled_event_created_at",
        "scheduled_event_end_time",
        "scheduled_event_start_time",
        "scheduled_event_updated_at",
        "updated_at"
    ]
    timestamp_format = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'"

    for c in timestamp_cols:
        df = df.withColumn(c, to_timestamp(col(c), timestamp_format))

    # Write to Parquet partition folder
    output_path = f"s3://{raw_bucket}/{processed_prefix}event_date={date_part}/"
    df.write.mode("append").parquet(output_path)
    print(f"✅ Wrote processed data to {output_path}")

    # Add partition to Athena
    partition_sql = f"""
        ALTER TABLE {athena_table}
        ADD IF NOT EXISTS
        PARTITION (event_date = '{date_part}')
        LOCATION 's3://{raw_bucket}/{processed_prefix}event_date={date_part}/'
    """
    athena.start_query_execution(
        QueryString=partition_sql,
        QueryExecutionContext={'Database': athena_db},
        ResultConfiguration={'OutputLocation': athena_output}
    )
    print(f"📌 Added partition to Athena for event_date={date_part}")

    # Delete raw JSON files in folder
    delete_objs = [{'Key': key} for key in json_files]
    if delete_objs:
        s3.delete_objects(Bucket=raw_bucket, Delete={'Objects': delete_objs})
        print(f"🗑️ Deleted raw files in folder {folder_path}")

sc.stop()
print("Glue job finished.")
