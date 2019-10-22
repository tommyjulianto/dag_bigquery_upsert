from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.email_operator import EmailOperator
from operators.pg_download_operator import PgDownloadOperator
from operators.spectre_operator import PostgresToGoogleCloudStorageOperator
from operators.dataproc_operator import DataProcPySparkOperator
from operators.dataproc_operator import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from google.cloud import storage, bigquery
import json

dt = datetime.now()

global_config = Variable.get("global_variable_dumping", deserialize_json = True)
local_config = Variable.get("test-bigquery-upsert", deserialize_json=True)   
default_args_config = local_config["default_args"]

def cek_fail_safe_and_set_last_sync():
    local_config = Variable.get("test-bigquery-upsert", deserialize_json=True)   
    if local_config["fail_safe"] == 1:
        local_config["extract_date"] = '1900-00-00 00:00:00.000000'
        local_config["last_sync"] = str(dt)
        local_config = json.dumps(local_config)
        Variable.set("test-bigquery-upsert", local_config)
                    
    else:
        local_config["extract_date"] = str(dt)
        local_config["last_sync"] = str(dt)
        local_config = json.dumps(local_config)
        Variable.set("test-bigquery-upsert", local_config)

def get_columns_name(**kwargs):
    client = storage.Client()
    bucket = client.get_bucket(global_config["bucket"])
    blob = bucket.get_blob(local_config["schema_file"])
    content = blob.download_as_string()

    json_data = json.loads(content)
    list_table_u = []
    list_table_i = []

    for d in json_data:
        isian_list_table_u = "{} = S.{}".format(d["name"], d["name"])
        isian_list_table_i = str(d["name"])

        list_table_u.append(isian_list_table_u)
        list_table_i.append(isian_list_table_i)

    list_to_string_u = ', '.join(map(str, list_table_u))
    list_to_string_i = ', '.join(map(str, list_table_i))

    kwargs['ti'].xcom_push(key='updated_fields', value=list_to_string_u)
    kwargs['ti'].xcom_push(key='inserted_fields', value=list_to_string_i)

def if_tbl_exists():
    from google.cloud.exceptions import NotFound
    client = bigquery.Client()
    dataset = client.dataset(local_config["bq_dataset_primary"])
    table_ref = dataset.table(local_config["bq_table_primary"])

    try:
        client.get_table(table_ref)
        print('table exist')
        return local_config["task"][8]["task_id"]
    except NotFound:
        print('table not exist')
        return local_config["task"][4]["task_id"]

def check_is_fail_safe():
    local_config = Variable.get("test-bigquery-upsert", deserialize_json=True)

    if local_config["fail_safe"] == 1:
        return local_config["task"][5]["task_id"]
    else:
        return local_config["task"][3]["task_id"]

def save_airflow_variable():
    local_config = Variable.get("test-bigquery-upsert", deserialize_json=True)
    local_config["fail_safe"] = 0
    local_config = json.dumps(local_config)
    Variable.set("test-bigquery-upsert", local_config)

default_args = {
   'owner': default_args_config["owner"],
   'depends_on_past': default_args_config["depends_on_past"],
   'start_date': datetime.strptime(default_args_config["start_date"], default_args_config["start_date_format"]),
   'email': [default_args_config["email"]],
   'email_on_failure': default_args_config["email_on_failure"],
   'email_on_retry': default_args_config["email_on_retry"],
   'retries': default_args_config["retries"],
   'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    local_config["dag_name"],
    default_args=default_args,
    catchup=default_args_config["catchup"],
    schedule_interval=default_args_config["schedule_interval"]
)

last_sync = PythonOperator(
    task_id=local_config["task"][0]["task_id"],
    python_callable= cek_fail_safe_and_set_last_sync,
    dag=dag
)

extract_users = PostgresToGoogleCloudStorageOperator(
    task_id = local_config["task"][1]["task_id"], 
    postgres_conn_id = global_config["postgres_conn_id"], ##create connection di airflow
    bucket = global_config["bucket"], ##create bucket di storage (gcs)
    filename = '{}/{}/{}/{}_{:%Y-%m-%d}'.format(global_config["dumping"], 
                                                local_config["department"], 
                                                local_config["name_report"], 
                                                local_config["name_report"],
                                                datetime.strptime(  local_config["last_sync"],
                                                                    "%Y-%m-%d %H:%M:%S.%f"
                                                                )
                                                ),
    sql=local_config["task"][1]["sql"],
    dag=dag
    )

pyspark_job = DataProcPySparkOperator(
    task_id=local_config["task"][2]["task_id"],
    main=local_config["task"][2]["pyspark_job"], #pyspark script location (gs://)
    cluster_name=local_config["task"][2]["cluster_name"],
    arguments=[local_config["extract_date"], local_config["last_sync"]],
    dag=dag            
    )

bq_load_to_staging_table = GoogleCloudStorageToBigQueryOperator(
    task_id=local_config["task"][3]["task_id"],
    bucket=global_config["bucket"],
    # pakai local config untuk filesource
    source_objects=["dump_report/data/bigquery_upsert_test/bigquery_upsert_dump_{:%Y-%m-%d}/*.csv".format(datetime.strptime(local_config["last_sync"], "%Y-%m-%d %H:%M:%S.%f"))],
    destination_project_dataset_table="{}:{}.{}".format(local_config["bq_destination"], local_config["bq_dataset_staging"], local_config["bq_table_staging"]),
    schema_fields=local_config["task"][3]["schema_fields"],
    schema_object=local_config["task"][3]["schema_object"],  # Relative gcs path to schema file.
    source_format=local_config["task"][3]["source_format"],  # Note that our spark job does json -> csv conversion.
    create_disposition=local_config["task"][3]["create_disposition"],
    skip_leading_rows=local_config["task"][3]["skip_leading_rows"],
    write_disposition=local_config["task"][3]["write_disposition"],  # If the table exists, overwrite it.
    max_bad_records=local_config["task"][3]["max_bad_records"],
    ignore_unknown_values=local_config["task"][3]["ignore_unknown_values"],
    field_delimiter=local_config["task"][3]["field_delimiter"],
    dag=dag
    )

bq_load_to_primary_table = GoogleCloudStorageToBigQueryOperator(
    task_id=local_config["task"][4]["task_id"],
    bucket=global_config["bucket"],
    # pakai local config untuk filesource
    source_objects=["dump_report/data/bigquery_upsert_test/bigquery_upsert_dump_{:%Y-%m-%d}/*.csv".format(datetime.strptime(local_config["last_sync"], "%Y-%m-%d %H:%M:%S.%f"))],
    destination_project_dataset_table="{}:{}.{}".format(local_config["bq_destination"], local_config["bq_dataset_primary"], local_config["bq_table_primary"]),
    schema_fields=local_config["task"][4]["schema_fields"],
    schema_object=local_config["task"][4]["schema_object"],  # Relative gcs path to schema file.
    source_format=local_config["task"][4]["source_format"],  # Note that our spark job does json -> csv conversion.
    create_disposition=local_config["task"][4]["create_disposition"],
    skip_leading_rows=local_config["task"][4]["skip_leading_rows"],
    write_disposition=local_config["task"][4]["write_disposition"],  # If the table exists, overwrite it.
    max_bad_records=local_config["task"][4]["max_bad_records"],
    ignore_unknown_values=local_config["task"][4]["ignore_unknown_values"],
    field_delimiter=local_config["task"][4]["field_delimiter"],
    dag=dag
    )

bq_load_to_primary_table_if_fail_save = GoogleCloudStorageToBigQueryOperator(
    task_id=local_config["task"][5]["task_id"],
    bucket=global_config["bucket"],
    # pakai local config untuk filesource
    source_objects=["dump_report/data/bigquery_upsert_test/bigquery_upsert_dump_{:%Y-%m-%d}/*.csv".format(datetime.strptime(local_config["last_sync"], "%Y-%m-%d %H:%M:%S.%f"))],
    destination_project_dataset_table="{}:{}.{}".format(local_config["bq_destination"], local_config["bq_dataset_primary"], local_config["bq_table_primary"]),
    schema_fields=local_config["task"][5]["schema_fields"],
    schema_object=local_config["task"][5]["schema_object"],  # Relative gcs path to schema file.
    source_format=local_config["task"][5]["source_format"],  # Note that our spark job does json -> csv conversion.
    create_disposition=local_config["task"][5]["create_disposition"],
    skip_leading_rows=local_config["task"][5]["skip_leading_rows"],
    write_disposition=local_config["task"][5]["write_disposition"],  # If the table exists, overwrite it.
    max_bad_records=local_config["task"][5]["max_bad_records"],
    ignore_unknown_values=local_config["task"][5]["ignore_unknown_values"],
    field_delimiter=local_config["task"][5]["field_delimiter"],
    dag=dag
    )

push_merged_fields = PythonOperator(
    task_id=local_config["task"][6]["task_id"],
    python_callable=get_columns_name,
    provide_context=local_config["task"][6]["provide_context"],
    dag=dag
)

check_is_table_exist = BranchPythonOperator(
    task_id=local_config["task"][7]["task_id"],
    python_callable=if_tbl_exists,
    dag=dag
)

check_is_fail_safe_method = BranchPythonOperator(
    task_id=local_config["task"][8]["task_id"],
    python_callable=check_is_fail_safe,
    dag=dag
)

merge_bq_data = BigQueryOperator(
    task_id=local_config["task"][9]["task_id"],
    use_legacy_sql=local_config["task"][9]["use_legacy_sql"],
    location=local_config["bq_location"],
    sql="""{}""".format('\n'.join(map(str, local_config["task"][9]["merge_query"])). \
                    format( local_config["bq_dataset_primary"], local_config["bq_table_primary"],
                            local_config["bq_dataset_staging"], local_config["bq_table_staging"],
                            local_config["bq_table_primary_key"], local_config["bq_table_primary_key"],
                            "{{ ti.xcom_pull(key='updated_fields', task_ids='push_merged_fields')}}", 
                            "{{ ti.xcom_pull(key='inserted_fields', task_ids='push_merged_fields')}}",
                            "{{ ti.xcom_pull(key='inserted_fields', task_ids='push_merged_fields')}}")
                        ),
    dag=dag
)

save_airflow_var = PythonOperator(
    task_id=local_config["task"][10]["task_id"],
    python_callable=save_airflow_variable,
    trigger_rule=TriggerRule.ONE_SUCCESS,
    dag=dag
)

last_sync >> extract_users >> pyspark_job >> check_is_table_exist >> [bq_load_to_primary_table, check_is_fail_safe_method]

check_is_fail_safe_method >> [bq_load_to_primary_table_if_fail_save, bq_load_to_staging_table]

bq_load_to_primary_table >> save_airflow_var

bq_load_to_primary_table_if_fail_save >> save_airflow_var

bq_load_to_staging_table >> push_merged_fields >> merge_bq_data >> save_airflow_var