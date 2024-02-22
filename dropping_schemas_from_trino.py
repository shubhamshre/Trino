import trino
from minio import Minio
from minio.error import MinioException

csv_files_list_minio = dict()

# MinIO connection details
minio_host = 'localhost:9000'
minio_access_key = 'minio_access_key'
minio_secret_key = 'minio_secret_key'
secure = False  # Change to True if MinIO is secured with TLS/SSL

# Initialize MinIO client
minio_client = Minio(minio_host, access_key=minio_access_key, secret_key=minio_secret_key, secure=secure)

def list_objects_recursive(minio_client, bucket_name, prefix=''):
    try:
        # List all objects in the bucket with the given prefix
        objects = minio_client.list_objects(bucket_name, prefix=prefix, recursive=True)
        for obj in objects:
            slash_index = obj.object_name.index('/')
            # print(f"- {obj.object_name[0:slash_index]}")
            if obj.object_name[0:slash_index] not in csv_files_list_minio:
                csv_files_list_minio[obj.object_name[0:slash_index]] = 0
    except MinioException as err:
        print(err)

    # List all sub-buckets (folders)
    for prefix in set(obj.object_name.split('/')[0] for obj in objects if '/' in obj.object_name):
        # print(f"Sub-bucket: {prefix}")
        list_objects_recursive(minio_client, bucket_name, prefix=prefix)

try:
    # List all buckets
    buckets = minio_client.list_buckets()
    for bucket in buckets:
        bucket_name = bucket.name
        if bucket_name=='test':
            # print(f"Files in bucket '{bucket_name}':")
            list_objects_recursive(minio_client, bucket_name)
            print()

except MinioException as err:
    print(err)

trino_conn = None

def get_trino_connection():
    global trino_conn
    if trino_conn == None:
        trino_conn = trino.dbapi.connect(
            host='localhost',
            port=8080,
            user='admin'
        )
    return trino_conn

cur = get_trino_connection().cursor()

for key,value in csv_files_list_minio.items():
    print('For {}'.format(key))
    # dropping minio table data
    cur.execute("""drop table {catalog_source}.{schema_source}.{table_source}""".format(
        catalog_source = 'minio',
        schema_source = key,
        table_source = key+'_data'
    ))
    # dropping minio schema
    cur.execute("""drop schema {catalog_source}.{schema_source}""".format(
        catalog_source = 'minio',
        schema_source = key
    ))
    # dropping iceberg table data
    cur.execute("""drop table {catalog}.{schema}.{table}""".format(
        catalog = 'iceberg',
        schema = key+'_iceberg',
        table = key+'_data_iceberg'
    ))
    # dropping iceberg schema
    cur.execute("""drop schema {catalog}.{schema}""".format(
        catalog = 'iceberg',
        schema = key+'_iceberg'
    ))
    query = cur.fetchall()
    print(query)
    print('Successful')