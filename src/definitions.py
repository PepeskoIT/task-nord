from clients.aws import get_bucket_and_boto3_from_url
from envs import S3_STORAGE_URL

BUCKET, BOTO3_CLIENT = get_bucket_and_boto3_from_url(S3_STORAGE_URL)
