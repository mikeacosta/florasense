import boto3
import json
import urllib.parse
import os

"""
    Lambda function for moving processed JSON files from source to destination
    Invoked after running ETL to avoid duplicate processing of files

    Root source and destination bucket folder are defined in the event payload
        {"source": "folder_A", "dest": "folder_B"}
"""

s3 = boto3.resource('s3')

bucketName = "real-time-sensors"
bucket = s3.Bucket(bucketName)

def main(event, context):
    source = event["source"]
    destination = event["dest"]

    if not source or not destination:
        print("invalid input")
        return

    if not source.endswith("/"):
        source = source + "/"

    if not destination.endswith("/"):
        destination = destination + "/"        

    for obj in bucket.objects.filter(Prefix=source):
        key = obj.key
        if key.endswith('.json'):  
            sourceFile = urllib.parse.unquote_plus(key, encoding='utf-8')
            destFile = key.replace(source, destination)
            s3.Object(bucketName, destFile).copy_from(CopySource = bucketName + "/" + sourceFile)
            s3.Object(bucketName, sourceFile).delete()
            print("moved to " + destFile)