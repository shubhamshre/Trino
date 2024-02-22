from minio import Minio
from minio.error import S3Error
import os

def upload_folder(minio_client, bucket_name, local_folder):
    for root, dirs, files in os.walk(local_folder):
        for file in files:
            local_file_path = os.path.join(root, file)
            remote_file_path = os.path.relpath(local_file_path, local_folder)
            try:
                minio_client.fput_object(bucket_name, remote_file_path, local_file_path)
                print(f"Uploaded {local_file_path} to {remote_file_path}")
            except S3Error as err:
                print(err)

# Initialize MinIO client
minio_client = Minio("localhost:9000",
                     access_key="minio_access_key",
                     secret_key="minio_secret_key",
                     secure=False)

# Set bucket name
bucket_name = "testing"

# Set local folder path
local_folder = "/Users/se/Documents/Programming/Trino/data"

# Call the function to upload the folder
upload_folder(minio_client, bucket_name, local_folder)
