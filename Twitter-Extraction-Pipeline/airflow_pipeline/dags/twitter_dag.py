# -- Import class from the file -- #
import sys
sys.path.append("airflow_pipeline")

# -- Join the path for the output file -- #
from os.path import join

# -- Set the data interval -- #
import pendulum

# -- DAG and Operator from Airflow -- #
from airflow.models import DAG
from operators.twitter_operator import TwitterOperator

"""The dag will start two days ago and its execution frequency is daily"""
with DAG(dag_id="TwitterDAG", start_date=pendulum.today("UTC").add(days=-2), schedule="@daily") as dag:
	# -- Personal Query -- #
	query = "data science"
	file_path = join("datalake/twitter_datascience", "extract_date={{ ds }}", "datascience_{{ ds_nodash }}.json")

	to = TwitterOperator(file_path=file_path,
	                     end_time="{{ data_interval_end.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}",
	                     start_time="{{ data_interval_start.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}",
	                     query=query,
	                     task_id="twitter_datascience")