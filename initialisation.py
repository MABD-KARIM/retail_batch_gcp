from Z_db import database


#1- Generate initial data
from A_generate_data import generate_customers, generate_orders_stocks, generate_products
generate_customers()
generate_products()
generate_orders_stocks()

#2- Create Posgresql database
from B_posgresql_ingestion import  postgre_initial_orders_stocks, postgre_initialisaion_customers_products,create_postgresql_database

db = database
create_postgresql_database(db)
postgre_initialisaion_customers_products(db)
postgre_initial_orders_stocks(db)

#3- Incremental refresh are done via cronjob

#4 - Export data from postgresql database
from C_export_postgre_initial import extract_csv, create_gcs_bucket, upload_csv_to_bucket

bucket_name="bucket_"+db
create_gcs_bucket(bucket_name)
tables = ["products","customers","stocks","orders"]
for table in tables :
    query = f"SELECT * FROM {table}"
    file_name=table+"_initial"
    local_file_path = extract_csv(query,file_name, db)
    upload_csv_to_bucket(bucket_name,local_file_path,file_name)


#5.1 - BQ Datset creation
from D_dataset_table_creation import create_dataset
dataset = create_dataset(db)

#5.2 - BQ table creation
from D_dataset_table_creation import create_table

schema_dict = dict()
schema_dict["orders"]=[
                ("orderid","INTEGER"),
                ("orderdate","DATE"),
                ("prodid","INTEGER"),
                ("custid", "INTEGER"),
                ("qty","INTEGER"),
                ("ishonored","BOOLEAN")
                ]
schema_dict["stocks"]= [
                ("stockdate","DATE"),
                ("prodid","INTEGER"),
                ("stockqty","INTEGER"),
                ]
schema_dict["products"]=[
                ("prodid","INTEGER"),
                ("prodcode","STRING"),
                ("prodbrand","STRING"),
                ("prodcost","INTEGER"),
                ("prodprice","INTEGER")
                    ]
schema_dict["customers"]=[
                ("custid","INTEGER"),
                ("custfirstname","STRING"),
                ("custlastname","STRING"),
                ("custcity","STRING")
                    ]
schema_dict["orderheaders"]= [
            ("orderHeaderId","INTEGER"),
            ("orderdate","DATE"),
            ("custid","INTEGER"),
            ("NbOrders", "INTEGER"),
            ("Qty","INTEGER"),
            ("QtyHonored","INTEGER")
            ]

partition_field = dict()
partition_field["orders"]="orderdate"
partition_field["stocks"]="stockdate"
partition_field["products"]=None
partition_field["customers"]=None
partition_field["orderheaders"]="orderdate"

for table in ["orders","stocks","products","customers", "orderheaders"]:
    create_table(db, table, schema_dict[table], partition_field[table])
    print("Success !!!")

#6 - initial upload to tables 
from E_upload_to_bq import upload_to_bq

upload_to_bq(dataset, "orders",bucket_name, "orders_initial")
upload_to_bq(dataset, "stocks", bucket_name,"stocks_initial")
upload_to_bq(dataset, "products", bucket_name, "products_initial")
upload_to_bq(dataset, "customers", bucket_name, "customers_initial")

# 7 execute apche beam pipeline and save data to BQ
from F_get_last_orderheader import get_last_header
from F_beam_pipeline import orders_pipeline

n = get_last_header(db)
orders_pipeline(bucket_name,"orders_initial", db, n)




