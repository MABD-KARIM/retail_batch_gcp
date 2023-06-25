from google.cloud import bigquery

def upload_to_bq(dataset, table_name, bucket_name, file_name):
    key_path = "serviceAccountKey.json"
    # intitialize the BQ Cliet
    client = bigquery.Client.from_service_account_json(key_path)
    # table and dataset
    dataset_name= dataset
    table_ref = client.dataset(dataset_name).table(table_name)
    uri = f"gs://{bucket_name}/{file_name}"
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


