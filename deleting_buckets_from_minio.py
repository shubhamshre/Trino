from minio import Minio
from minio.error import S3Error

# MinIO connection details
minio_host = 'localhost:9000'
minio_access_key = 'minio_access_key'
minio_secret_key = 'minio_secret_key'
secure = False  # Change to True if MinIO is secured with TLS/SSL

# Initialize MinIO client
minio_client = Minio(minio_host, access_key=minio_access_key, secret_key=minio_secret_key, secure=secure)

def delete_all_objects_in_bucket(minio_client, bucket_name):
    try:
        objects = minio_client.list_objects(bucket_name, recursive=True)
        for obj in objects:
            minio_client.remove_object(bucket_name, obj.object_name)
            print(f"Object '{obj.object_name}' deleted successfully from bucket '{bucket_name}'")
    except S3Error as err:
        print(err)

def delete_all_buckets(minio_client):
    try:
        buckets = minio_client.list_buckets()
        for bucket in buckets:
            bucket_name = bucket.name
            if bucket_name == 'test':
                delete_all_objects_in_bucket(minio_client, bucket_name)
                minio_client.remove_bucket(bucket_name)
                print(f"Bucket '{bucket_name}' and its contents deleted successfully")
    except S3Error as err:
        print(err)

# Example usage
delete_all_buckets(minio_client)
