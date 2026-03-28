import json
import os
import boto3

s3 = boto3.client('s3')

VALID_EXTENSIONS = ['.jpg', '.jpeg', '.png', '.gif']

def is_valid_image(key):
    """check if the file has a valid image extension or not."""
    _, ext = os.path.splitext(key.lower())
    return ext in VALID_EXTENSIONS

def lambda_handler(event, context):
    """
    Here we validate if uploaded files are images.
    raises exception for invalid files and triggers DLQ.

    for valid files, copies the object to the processed/valid/ prefix
    in the same bucket so the grading can verify output via S3.

    event structure in SNS wraps the S3 event:
    {
        "Records": [{
            "Sns": {
                "Message": "{\"Records\":[{\"s3\":{...}}]}"  # this is a JSON string!
            }
        }]
    }

    required log format:
        [VALID] {key} is a valid image file
        [INVALID] {key} is not a valid image type

    required S3 output of valid files only:
        copies the file to processed/valid/{filename}
        e.g. uploads/test.jpg -> processed/valid/test.jpg

    important: to trigger the DLQ, you must raise an exception and not return an error.
    """

    print("=== image validator invoked ===")

    # Parse the SNS wrapper to get the actual S3 event
    for record in event['Records']:
        sns_message = record['Sns']['Message']
        # SNS Message is a JSON string, need to parse it
        s3_event = json.loads(sns_message)
        
        # Must process each S3 upload event
        for s3_record in s3_event['Records']:
            bucket = s3_record['s3']['bucket']['name']
            key = s3_record['s3']['object']['key']
            
            # We validate file extension
            if is_valid_image(key):
                print(f"[VALID] {key} is a valid image file")
                
                # Extract just the filename from the full key path
                filename = key.split('/')[-1]
                
                # Copy valid file to processed/valid/ prefix
                s3.copy_object(
                    Bucket=bucket,
                    Key=f"processed/valid/{filename}",
                    CopySource={'Bucket': bucket, 'Key': key}
                )
            else:
                # Log invalid file and raise exception to trigger DLQ
                print(f"[INVALID] {key} is not a valid image type")
                raise ValueError(f"Invalid file type: {key}")

    return {'statusCode': 200, 'body': 'validation complete'}
