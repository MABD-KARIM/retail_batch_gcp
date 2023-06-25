# Python Functions
def get_last_date_from_table():
    from google.cloud import bigquery
    from google.oauth2 import service_account
    from datetime import timedelta

    dataset_id="mkarim"
    credentials = service_account.Credentials.from_service_account_file('/opt/airflow/dags/serviceAccountKey.json')
    project_id = "my-learning-375919"
    bq_client = bigquery.Client(project=project_id,credentials=credentials)
    query = f"""
            SELECT MAX(orderdate) AS last_date
            FROM `{project_id}.{dataset_id}.orders`
        """
    query_job = bq_client.query(query)
    results = query_job.result()
    for row in results:
        last_date = row.last_date
    last_date += timedelta(days=1)
    return last_date


def extract_csv_orders(last_date):
    import psycopg2
    import csv

    str_date = "'"+str(last_date)+"'"
    file_name="orders"+"_"+str(last_date)
    query = f"SELECT * FROM orders WHERE orderdate={str_date}"
    # PostgreSQL connection details
    host = '192.168.1.13'
    database = "mkarim"
    user = 'postgres'
    password = '1234'
    # Establish a connection to the PostgreSQL database
    connection = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password
    )
    # Create a cursor object to interact with the database
    cursor = connection.cursor()
    # Execute the query
    cursor.execute(query)
    # Fetch all the rows returned by the query
    rows = cursor.fetchall()
    # Define the CSV file path
    csv_file_path = '/opt/airflow/dags/files/'+file_name+'.csv'
    # Open the CSV file in write mode
    with open(csv_file_path, 'w', newline='') as csvfile:
        # Create a CSV writer object
        csv_writer = csv.writer(csvfile)
        # Write the column headers to the CSV file
        csv_writer.writerow([desc[0] for desc in cursor.description])
        # Write each row to the CSV file
        csv_writer.writerows(rows)
    print(f"We have extracted the {file_name} table with {len(rows)} line")
    cursor.close()
    connection.close()
    return [csv_file_path, file_name]

def extract_csv_stocks(last_date):
    import psycopg2
    import csv

    str_date = "'"+str(last_date)+"'"
    file_name="stocks"+"_"+str(last_date)
    query = f"SELECT * FROM stocks WHERE stockdate={str_date}"
    # PostgreSQL connection details
    host = '192.168.1.13'
    database = "mkarim"
    user = 'postgres'
    password = '1234'
    # Establish a connection to the PostgreSQL database
    connection = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password
    )
    # Create a cursor object to interact with the database
    cursor = connection.cursor()
    # Execute the query
    cursor.execute(query)
    # Fetch all the rows returned by the query
    rows = cursor.fetchall()
    # Define the CSV file path
    csv_file_path = '/opt/airflow/dags/files/'+file_name+'.csv'
    # Open the CSV file in write mode
    with open(csv_file_path, 'w', newline='') as csvfile:
        # Create a CSV writer object
        csv_writer = csv.writer(csvfile)
        # Write the column headers to the CSV file
        csv_writer.writerow([desc[0] for desc in cursor.description])
        # Write each row to the CSV file
        csv_writer.writerows(rows)
    print(f"We have extracted the {file_name} table with {len(rows)} line")
    cursor.close()
    connection.close()
    return [csv_file_path, file_name]

def upload_to_gcs(file_infos):
    from google.cloud import storage

    name = "bucket_mkarim"
    key_path = "/opt/airflow/dags/serviceAccountKey.json"
    client = storage.Client.from_service_account_json(key_path)
    bucket = client.get_bucket(name)
    blob = bucket.blob(file_infos[1])
    blob.upload_from_filename(file_infos[0])
    print(f"File uploaded to Cloud Storage bucket: gs://{name}/{file_infos[1]}")
    return f"gs://{name}/{file_infos[1]}"

def upload_to_bq_orders(uri):
    from google.cloud import bigquery

    table_name = "orders"
    key_path = "/opt/airflow/dags/serviceAccountKey.json"
    # intitialize the BQ Cliet
    client = bigquery.Client.from_service_account_json(key_path)
    # table and dataset
    dataset_name= "mkarim"
    table_ref = client.dataset(dataset_name).table(table_name)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.skip_leading_rows=1
    job_config.autodetect=True
    #start load job
    load_job = client.load_table_from_uri(uri,table_ref, job_config=job_config)
    load_job.result()

    # check for status
    if load_job.state == 'DONE':
        print("Data loaded successfully")
    else:
        print("There is an error in the job")

def upload_to_bq_stocks(uri):
    from google.cloud import bigquery

    table_name = "stocks"
    key_path = "/opt/airflow/dags/serviceAccountKey.json"
    # intitialize the BQ Cliet
    client = bigquery.Client.from_service_account_json(key_path)
    # table and dataset
    dataset_name= "mkarim"
    table_ref = client.dataset(dataset_name).table(table_name)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.skip_leading_rows=1
    job_config.autodetect=True
    #start load job
    load_job = client.load_table_from_uri(uri,table_ref, job_config=job_config)
    load_job.result()

    # check for status
    if load_job.state == 'DONE':
        print("Data loaded successfully")
    else:
        print("There is an error in the job")

def get_last_orderheader():
    from google.cloud import bigquery
    key_path = "/opt/airflow/dags/serviceAccountKey.json"
    # intitialize the BQ Cliet
    client = bigquery.Client.from_service_account_json(key_path)
    # table and dataset
    dataset_name= "mkarim"
    table_ref = client.dataset(dataset_name).table("orderheaders")
    # define query 
    query = f"SELECT IFNULL(MAX(orderHeaderId),0) AS max_id FROM {table_ref}"
    # exceute query
    query_job = client.query(query)
    # Fetch results
    result = query_job.result()
    #get max id
    for row in result:
        max_id = row.max_id
    print(max_id)
    return max_id


def headers_pipeline(uri,n):
    import os
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/opt/airflow/dags/serviceAccountKey.json"
    db = "mkarim"
    import apache_beam as beam
    from apache_beam.options.pipeline_options import PipelineOptions
    pipeline_options = PipelineOptions(runner = "DirectRunner", temp_location="gs://temperoray_files")
    class My_func(beam.DoFn):
        id = n
        def process(self, input):
            self.id += 1
            input_list = input.split(",")
            input = ",".join(input_list)
            line = str(self.id)+","+input
            return [line]

    def combine_values(values):
        sum_first = sum(v[0] for v in values)
        sum_second = sum(v[1] for v in values)
        sum_third = sum(v[2] for v in values)
        return sum_first, sum_second, sum_third

    def map_func(input):
        line = input.split(",")
        key = str(line[1])+"_"+str(line[3])
        qty_honored = int(line[4]) if line[5]=="True" else 0
        qty = int(line[4])
        values = (1,qty,qty_honored)
        return (key, values)

    def flatten_result(input):
        key, values = input
        key = key.split("_")
        date=key[0]
        cust = key[1]
        return ",".join([str(date), str(cust),str(values[0]),str(values[1]),str(values[2])])

            
    bq_table = 'my-learning-375919:'+db+".orderheaders"
    
    input_gcs = uri
    output_gcs = f"gs://bucket_mkarim/orderheaders_{n}"
    with beam.Pipeline(options=pipeline_options) as p_1 :
   

        add_id = (
            p_1
            | "read csv file" >> beam.io.ReadFromText(input_gcs,skip_header_lines=1)
            | "mapping every line to key value pair" >> beam.Map(map_func)
            | "reduce results" >> beam.CombinePerKey(combine_values)
            | "faltten results" >> beam.Map(flatten_result)
            | "add id to each line" >> beam.ParDo(My_func())
            | "write to gcs" >> beam.io.WriteToText(output_gcs, file_name_suffix=".csv",num_shards=1)
        )
    print("Pipeline ran succssfully !!")

    with beam.Pipeline(options=pipeline_options) as p_2 :
   

        insert_bq = (
            p_2
            | "Read from gcs" >> beam.io.ReadFromText(output_gcs+"-00000-of-00001.csv")
            | "Parse CSV" >> beam.Map(lambda line: line.split(","))
            | "Format Output" >> beam.Map(lambda fields: {"orderHeaderId": fields[0], "orderdate": fields[1], "custid": fields[2],"NbOrders": fields[3], "Qty": fields[4], "QtyHonored" : fields[5]})
            | "Write to BigQuery" >> beam.io.WriteToBigQuery(
            table=bq_table,
            schema="orderHeaderId:INTEGER,orderdate:DATE,custid:INTEGER,NbOrders:INTEGER,Qty:INTEGER,QtyHonored:INTEGER",
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            custom_gcs_temp_location="gs://temperoray_files"
            )
        )
    print("Pipeline ran succssfully !!")








