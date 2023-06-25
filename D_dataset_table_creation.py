from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.bigquery import SchemaField


def create_dataset(dataset_name):
    # Path to your service account key file
    service_account_key_path = "serviceAccountKey.json"

    # Create a BigQuery client using service account credentials
    credentials = service_account.Credentials.from_service_account_file(service_account_key_path)
    client = bigquery.Client(credentials=credentials)

    # Name of the project where the dataset will be created
    project_id = 'my-learning-375919'


    # Construct the dataset reference
    dataset_ref = client.dataset(dataset_name, project=project_id)

    # Define dataset metadata
    dataset = bigquery.Dataset(dataset_ref)
    dataset.description = 'A new dataset created via Python code'
    dataset.location = 'US'  # Set the desired location for the dataset

    # Create the dataset
    client.create_dataset(dataset, exists_ok=True)

    print(f"Dataset '{dataset}' created successfully.")
    return dataset_name


def create_table(dataset, table_name, schema_list , partition_field=None):
    key_path = "serviceAccountKey.json"
    # intitialize the BQ Cliet
    client = bigquery.Client.from_service_account_json(key_path)
    # table and dataset
    dataset_name= dataset
    table_ref = client.dataset(dataset_name).table(table_name)
    try :
        table = client.get_table(table_ref)
        print(f"table {table_name} already exists !")
    except Exception :
        # Define table schema 
        print("I am creating a table")
        schema = []
        for element in schema_list:
            schema.append(SchemaField(element[0],element[1]))

        table = bigquery.Table(table_ref,schema)
        if table_name in ["orders", "stocks","orderheaders"]:
            table.time_partitioning = bigquery.TimePartitioning(field=partition_field)
        table = client.create_table(table)
        print(f"Table {table_name} has been created successfully")
    
