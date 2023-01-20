# -- Import class from the file -- #
import sys

sys.path.append("airflow_pipeline")

# -- Join the path for the output file -- #
from os.path import join
from pathlib import Path

# -- Set the data interval -- #
import pendulum

# -- DAG and Operator from Airflow -- #
from airflow.models import DAG
from operators.twitter_operator import TwitterOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

"""The dag will start two days ago and its execution frequency is daily"""
with DAG(dag_id="TwitterDAG", start_date=pendulum.today("UTC").add(days=-2), schedule="@daily") as dag:
	BASE_FOLDER = join(
		str(Path("~/Documents").expanduser()),
		"Data-Extraction-Pipeline/datalake/{stage}/twitter_datascience/{partition}"
	)
	PARTITION_FOLDER_EXTRACT = "extract_date={{ data_interval_start.strftime('%Y-%m-%d') }}"
	# -- Personal Query -- #
	query = "data science"
	file_path = join(BASE_FOLDER.format(stage="bronze", partition=PARTITION_FOLDER_EXTRACT),
	                 "datascience_{{ ds_nodash }}.json")

	twitter_operator = TwitterOperator(file_path=file_path,
	                                   end_time="{{ data_interval_end.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}",
	                                   start_time="{{ data_interval_start.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}",
	                                   query=query,
	                                   task_id="twitter_datascience")

	twitter_transform = SparkSubmitOperator(task_id="transform_twitter_datascience",
	                                        application="source/spark/spark_transformation.py",
	                                        name="twitter_transformation",
	                                        application_args=["--src", BASE_FOLDER.format(stage="bronze", partition=PARTITION_FOLDER_EXTRACT),
	                                                          "--destiny", BASE_FOLDER.format(stage="silver", partition=""),
	                                                          "--process-date", "{{ ds }}"])

	twitter_insight = SparkSubmitOperator(task_id="insight_twitter",
	                                      application="source/spark/insight_tweet.py",
	                                      name="insight_twitter",
	                                      application_args=["--src", BASE_FOLDER.format(stage="silver", partition=""),
	                                                        "--destiny", BASE_FOLDER.format(stage="gold", partition=""),
	                                                        "--process-date", "{{ ds }}"])

	twitter_operator >> twitter_transform >> twitter_insight
