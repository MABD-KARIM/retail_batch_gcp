import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions





def orders_pipeline(bucket_name,file_name,db,n):
    pipeline_options = PipelineOptions(runner = "DirectRunner", temp_location="gs//:temp_pipeline")
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
    
    input_gcs = f"gs://{bucket_name}/{file_name}"
    output_gcs = f"gs://{bucket_name}/orderheaders_{n}"
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


