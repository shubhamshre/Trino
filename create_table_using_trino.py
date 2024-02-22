import trino
from minio import Minio
from minio.error import MinioException
import time

csv_files_list_minio = dict()

# variables
minio_bucket_name = 'gds'

# MinIO connection details
minio_host = 'localhost:9000'
minio_access_key = 'minio_access_key'
minio_secret_key = 'minio_secret_key'
secure = False  # Change to True if MinIO is secured with TLS/SSL
# Initialize MinIO client
minio_client = Minio(minio_host, access_key=minio_access_key, secret_key=minio_secret_key, secure=secure)

#  read first line from csv files stored in minio to get the column names for creating the table
def read_csv_first_line(minio_client, bucket_name, object_name):
    try:
        # Get the object (CSV file)
        csv_file = minio_client.get_object(bucket_name, object_name)
        # Read the first line
        first_line = csv_file.readline().decode('utf-8').strip()
        # Close the object
        csv_file.close()
        return first_line
    except MinioException as err:
        print(err)
# reading all CSV files and storing their names and column names (first line) in a dictionary where key is filename without extension and value is list of columns
def list_objects_recursive(minio_client, bucket_name, prefix=''):
    try:
        # List all objects in the bucket with the given prefix
        objects = minio_client.list_objects(bucket_name, prefix=prefix, recursive=True)
        for obj in objects:
            slash_index = obj.object_name.index('/')
            # print(f"- {obj.object_name[0:slash_index]}")
            if obj.object_name[0:slash_index] not in csv_files_list_minio:
                csv_files_list_minio[obj.object_name[0:slash_index]] = 0
            if obj.object_name.endswith('.csv'):
                first_line = read_csv_first_line(minio_client, bucket_name, obj.object_name)
                csv_files_list_minio[obj.object_name[0:slash_index]] = first_line
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
        if bucket_name==minio_bucket_name:
            # print(f"Files in bucket '{bucket_name}':")
            list_objects_recursive(minio_client, bucket_name)
            print()

except MinioException as err:
    print(err)

# trino connection variable
trino_conn = None
# trino connection
def get_trino_connection():
    global trino_conn
    if trino_conn == None:
        trino_conn = trino.dbapi.connect(
            host='localhost',
            port=8080,
            user='admin'
        )
    return trino_conn
# trino connection start
cur = get_trino_connection().cursor()

# iterating through all the csv files, key is file name and value is columns list
for key,value in csv_files_list_minio.items():
    print('Starting for {}'.format(key))
    # adding varchar at the end of every column name to create table
    list_of_columns = value.replace(',', ' varchar, ')
    # creating minio schema
    cur.execute("""CREATE SCHEMA {catalog_source}.{schema_source} WITH (location = 's3a://{minio}/{folder}/')""".format(
        catalog_source = 'minio',
        schema_source = key,
        minio = minio_bucket_name,
        folder = key
    ))
    time.sleep(2)
    # creating minio table
    cur.execute("""CREATE TABLE {catalog_source}.{schema_source}.{table_source} ( {columns} varchar )
    WITH (
    skip_header_line_count = 1,
    external_location = 's3a://{minio}/{folder}/',
    format = 'CSV'
    )""".format(
        catalog_source = 'minio',
        schema_source = key,
        table_source = key+'_data',
        minio = minio_bucket_name,
        columns = list_of_columns,
        folder = key
    ))
    time.sleep(3)
    # viewing minio table data
    cur.execute("""select * from {catalog_source}.{schema_source}.{table_source}""".format(
        catalog_source = 'minio',
        schema_source = key,
        table_source = key+'_data'
    ))
    time.sleep(2)
    # creating iceberg schema
    cur.execute("""CREATE SCHEMA {catalog}.{schema} WITH (location = 's3a://{minio}/{folder}/')""".format(
        catalog = 'iceberg',
        schema = key+'_iceberg',
        minio = minio_bucket_name,
        folder = key
    ))
    time.sleep(2)
    # creating iceberg table
    cur.execute("""CREATE TABLE {catalog}.{schema}.{table}
    WITH (
    location = 's3a://{minio}/{folder}/',
    format = '{format}'
    ) as 
    select * from {catalog_source}.{schema_source}.{table_source}""".format(
        catalog = 'iceberg',
        catalog_source = 'minio',
        schema =  key+'_iceberg',
        schema_source = key,
        table = key+'_data_iceberg',
        table_source = key+'_data',
        minio = minio_bucket_name,
        format = 'PARQUET',
        folder = key
    ))
    time.sleep(3)
    # viewing iceberg table data
    cur.execute("""select * from {catalog}.{schema}.{table}""".format(
        catalog = 'iceberg',
        schema = key+'_iceberg',
        table = key+'_data_iceberg'
    ))
    time.sleep(2)
    query = cur.fetchall()
    print(query)
    print('Successful for {}'.format(key))