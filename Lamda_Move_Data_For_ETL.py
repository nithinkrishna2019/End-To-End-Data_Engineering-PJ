import boto3
import os

def lambda_handler(event,context):

    s3_client = boto3.client("s3")

    # S3 Bucket and Folder Details
    bucket_name = "aws-glue-s3-bucket"
    source_prefix = "End-to-End-PJ/source-json-data/"
    destination_prefix = "End-to-End-PJ/source-Data-For-ETL/"

    # List objects in the source folder
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=source_prefix)

    if "Contents" in response:
        file_names = []  # Empty list to store valid JSON file names

        for obj in response["Contents"]:
            file_key = obj["Key"]  # Get the file path from S3
            
            # Ignore the folder itself and only select .json files
            if file_key != source_prefix and file_key.endswith(".json"):
                file_names.append(file_key)

        for i in file_names:
            file_name_final = os.path.basename(i)  # Extract only filename
            destination_key = destination_prefix + file_name_final  # New S3 path

            # Copy file to the destination
            s3_client.copy_object(
                Bucket=bucket_name,
                CopySource={'Bucket': bucket_name, 'Key': i},
                Key=destination_key
            )
            print(f"Copied: {i} -> {destination_key}")

            # Delete file from the source
            s3_client.delete_object(Bucket=bucket_name, Key=i)
            print(f"Deleted: {i}")

        return {"Moved Files": file_names}
    else:
        print("No files found in the source folder.")
        return {"Moved Files": []}
