from google.cloud import bigquery




def get_last_header(dataset):
    key_path = "serviceAccountKey.json"
    # intitialize the BQ Cliet
    client = bigquery.Client.from_service_account_json(key_path)
    # table and dataset
    dataset_name= dataset
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

