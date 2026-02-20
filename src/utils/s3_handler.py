import boto3
import json
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class S3Handler:

    def __init__(self, bucket_name, region):
        self.bucket_name = bucket_name
        self.region = region
        self.s3_client = boto3.client('s3', region_name=region)
        logger.info(f"Initialized S3 handler for bucket: {bucket_name}")

    def upload_json(self, data, prefix, filename=None):
        try:
            # Generate filename if not provided
            if filename is None:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"crypto_data_{timestamp}.json"

            # Full S3 path
            s3_key = f"{prefix}{filename}"

            # Convert data to JSON string
            json_data = json.dumps(data, indent=2, default=str)

            # Upload to S3
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=json_data,
                ContentType='application/json'
            )

            logger.info(f"Uploaded to S3: s3://{self.bucket_name}/{s3_key}")
            return s3_key

        except Exception as e:
            logger.error(f"Error uploading to S3: {e}")
            raise