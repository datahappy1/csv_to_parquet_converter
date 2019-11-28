""" AWS Library """
import os
import logging
import sys
import boto3
from boto3.s3.transfer import S3Transfer
from boto3.exceptions import S3UploadFailedError
from botocore.exceptions import ClientError


logging.getLogger().setLevel(logging.INFO)


def init_s3():
    """
    Initiate AWS S3 bucket
    :return: conn_s3 on success, sys.exit on error
    """
    try:
        aws_access_key_id = os.environ.get("aws_access_key_id")
        aws_secret_access_key = os.environ.get("aws_secret_access_key")

        conn_s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id,
                               aws_secret_access_key=aws_secret_access_key)

        logging.info('AWS S3 set boto3.client success')
        return conn_s3
    except ClientError:
        logging.error('AWS S3 set boto3.client failed, ClientError')
        sys.exit(1)


def check_bucket(conn_s3):
    """
    Check if the s3 bucket set in settings.py exists and is available
    :param conn_s3
    :return: 0 if the bucket exists and we can access it
    """
    # try:
    #     bucket_name = S3_BUCKET_NAME
    #     conn_s3.head_bucket(Bucket=bucket_name)
    #     return 0
    # except ClientError:
    #     logging.error('AWS S3 bucket used in settings.py not exists or not available')
    #     sys.exit(1)


def download_from_s3(conn_s3, remote_file_name, local_file_name):
    # def get_s3_file(source_bucket, source_key):
    #     """
    #     get file from s3 and stream it to pandas df
    #     """
    #     session = boto3.Session(region_name='us-east-1', profile_name=gv.aws_profile)
    #     s3_client = session.client('s3')
    #
    #     source_bucket = AWS.__replace_env_variables(source_bucket)
    #
    #     try:
    #         response = s3_client.get_object(Bucket=source_bucket, Key=source_key)
    #         content = response['Body'].read()
    #         return content
    #     except ClientError as e:
    #         logging.info("Error getting this file from s3: %s ", source_key)
    #         logging.exception(e)

def upload_to_s3(conn_s3, local_file_name, remote_file_name):
    """
    Upload to S3
    :param conn_s3
    :param full_file_name:
    :param file_name:
    :return: 0 on success, sys.exit on error
    """
    # try:
    #     bucket_name = S3_BUCKET_NAME
    #     target_dir = S3_TARGET_DIR
    #
    #     transfer = S3Transfer(conn_s3)
    #     transfer.upload_file(full_file_name, bucket_name, target_dir + "/" + file_name)
    #     logging.info('%s file uploaded successfully, target bucket: %s, target folder: %s',
    #                  file_name, bucket_name, target_dir)
    #     return 0
    # except S3UploadFailedError:
    #     logging.error('AWS S3 upload failed, S3UploadFailedError')
    #     sys.exit(1)
