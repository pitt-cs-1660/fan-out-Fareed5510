import json
import os
import boto3
from datetime import datetime

s3 = boto3.client('s3')

def lambda_handler(event, context):
    """
    It extracts metadata from S3 upload events received via SNS.
    logs file information to CloudWatch and writes a JSON metadata
    file to the processed/metadata/ prefix in the same bucket.

    event structure in which SNS wraps the S3 event:
    {
        "Records": [{
            "Sns": {
                "Message": "{\"Records\":[{\"s3\":{...}}]}"  # this is a JSON string!
            }
        }]
    }

    required log format:
        [METADATA] File: {key}
        [METADATA] Bucket: {bucket}
        [METADATA] Size: {size} bytes
        [METADATA] Upload Time: {timestamp}

    the required S3 output:
        writes a JSON file to processed/metadata/{filename}.json containing:
        {
            "file": "{key}",
            "bucket": "{bucket}",
            "size": {size},
            "upload_time": "{timestamp}"
        }
    """

    print("=== metadata extractor invoked ===")

    # Parse the SNS wrapper to get the actual S3 event
    for record in event['Records']:
        sns_message = record['Sns']['Message']
        # SNS Message is a JSON string, we need to parse it
        s3_event = json.loads(sns_message)
        
        # Now we process each S3 upload event
        for s3_record in s3_event['Records']:
            bucket = s3_record['s3']['bucket']['name']
            key = s3_record['s3']['object']['key']
            size = s3_record['s3']['object']['size']
            event_time = s3_record['eventTime']
            
            # Log metadata in required format for CloudWatch
            print(f"[METADATA] File: {key}")
            print(f"[METADATA] Bucket: {bucket}")
            print(f"[METADATA] Size: {size} bytes")
            print(f"[METADATA] Upload Time: {event_time}")
            
            # Build metadata object for JSON output
            metadata = {
                "file": key,
                "bucket": bucket,
                "size": size,
                "upload_time": event_time
            }
            
            # Extract filename without extension (e.g., "test.jpg" -> "test")
            filename = key.split('/')[-1]
            filename_no_ext = os.path.splitext(filename)[0]
            
            # Write metadata JSON to processed/metadata/ prefix
            s3.put_object(
                Bucket=bucket,
                Key=f"processed/metadata/{filename_no_ext}.json",
                Body=json.dumps(metadata),
                ContentType='application/json'
            )
            
            print(f"[METADATA] Wrote metadata to processed/metadata/{filename_no_ext}.json")

    return {'statusCode': 200, 'body': 'metadata extracted'}
