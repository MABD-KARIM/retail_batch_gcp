from google.cloud import bigquery

def upload_to_bq(dataset, table, schema_list):
    key_path = "/home/mkarim/Desktop/serviceAccountKey.json"
    # intitialize the BQ Cliet
    client = bigquery.Client.from_service_account_json(key_path)
    # table and dataset
    dataset_name= dataset
    table_name=table
    table_ref = client.dataset(dataset_name).table(table_name)
    print(table_ref)
    try :
        table = client.get_table(table_ref)
        from google.cloud.bigquery import SchemaField
        # Define table schema 
        schema = []
        for element in schema_list:
            schema.append(SchemaField(element[0],element[1]))
        
        table = bigquery.Table(table_ref,schema)
        table = client.create_table(table)
        print(type(table))
        print("Table {table_name} has been created successfully")
        
    except :
        print('Table already exists in BigQuery. Skipping table creation.')
    # read csv file
    filename = "sources/"+table.lower()+".csv"
    with open(filename, mode="rb") as source_file:
        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.skip_leading_rows=1
        job_config.autodetect=True
        #start load job
        load_job = client.load_table_from_file(source_file, table_ref,job_config=job_config)
        load_job.result()

    # check for status
    if load_job.state == 'DONE':
        print("Data loaded successfully")
    else:
        print("There is an error in the job")


# Upload Customers :
table = "Customers"
dataset = "retail_test"
schema_list = [
    ("cusId","INTEGER"),
    ("cusFirstName","STRING"),
    ("cusLastName","STRING"),
    ("custCity","STRING")
          ]

upload_to_bq(dataset,table,schema_list)

# Upload Products :
table = "Products"
dataset = "retail_test"
schema_list = [
    ("productId","INTEGER"),
    ("productCode","STRING"),
    ("productBrand","STRING"),
    ("produCost","INTEGER"),
    ("prodPrice","INTEGER")
          ]
upload_to_bq(dataset, table, schema_list)

