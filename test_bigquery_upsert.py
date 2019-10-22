#!/usr/bin/python

from pyspark import SparkContext

from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F
from pyspark.sql.functions import when,regexp_replace 
import json
import os
from datetime import datetime
from google.cloud import storage
import sys

dt = datetime.strptime(sys.argv[2], "%Y-%m-%d %H:%M:%S.%f")
sc = SparkContext()

## read_files & read dataframe

sqlContext = SQLContext(sc)
upsert_data = sqlContext.read.json('gs://spectre-dump-staging/dump_report/data/bigquery_upsert_test/bigquery_upsert_test_{:%Y-%m-%d}'.format(dt))

new_df = upsert_data.withColumn('updated_at',upsert_data['updated_at'].cast('int')). \
                    withColumn('created_at',upsert_data['created_at'].cast('int'))

new_df = new_df.withColumn('updated_at',new_df['updated_at'].cast('timestamp')). \
                withColumn('created_at',new_df['created_at'].cast('timestamp'))

new_df.createOrReplaceTempView("new_df_table")
query = """select 
            id, 
            email, 
            created_at, 
            updated_at 
            from new_df_table where cast(updated_at as string) >= '{}' order by updated_at desc""".format(sys.argv[1])

print(query)

new_df = sqlContext.sql(query)

new_df.write.option("sep","|").csv('gs://spectre-dump-staging/dump_report/data/bigquery_upsert_test/bigquery_upsert_dump_{:%Y-%m-%d}'.format(dt), header=False )

### create schema from dataframe (import json)
def save_schema(dataset, schema_name):
    schema_raw = json.loads(dataset.schema.json())
    for s in schema_raw["fields"]:
        s.pop("metadata")
        s.pop("nullable")

        if s["type"] == "long":
            s["type"] = "integer"
        elif s["type"] == "double":
            s["type"] = "float"
        
    schema_json = json.dumps(schema_raw["fields"])
    print(schema_json)
    file_json = open('./schema_{}.json'.format(schema_name),'w') 
    file_json.write(schema_json)


save_schema(new_df, 'upsert_data')

os.system('gsutil cp ./schema_upsert_data.json gs://spectre-dump-staging/schemas/')
os.remove('./schema_upsert_data.json')

