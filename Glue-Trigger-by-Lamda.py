import boto3

glue_client = boto3.client('glue')

def lambda_handler(event, context):
    try:
        # Extract bucket name and object key from the S3 event
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        object_key = event['Records'][0]['s3']['object']['key']
        
        print(f"New file {object_key} detected in bucket {bucket_name}. Triggering Glue job...")

        # Start the Glue job
        response = glue_client.start_job_run(JobName="Weather-ETL-Job")

        print(f"Glue job started: {response['JobRunId']}")
        
        return {
            'statusCode': 200,
            'body': f"Glue job started successfully: {response['JobRunId']}"
        }
    
    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': f"Failed to start Glue job: {str(e)}"
        }
