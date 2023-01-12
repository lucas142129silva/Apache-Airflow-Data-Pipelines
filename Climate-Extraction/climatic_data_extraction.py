# -- Airflow -- #
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.macros import ds_add

# -- Time -- #
import pendulum

# -- System -- #
from os.path import join

# -- Data Manipulation -- #
import pandas as pd


# -- Function to extract the weather data from visual crossing -- #
def extract_data(data_interval_end):
	city = "Boston"
	key = "{{API_KEY}}"

	URL = join("https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/",
	           f"{city}/{data_interval_end}/{ds_add(data_interval_end, 7)}?unitGroup=metric&include=days&key={key}&contentType=csv")

	data = pd.read_csv(URL)

	filepath = f"./week={data_interval_end}/"

	data.to_csv(filepath + "raw_data.csv")
	data[["datetime", "tempmin", "temp", "tempmax"]].to_csv(filepath + "temperatures.csv")
	data[["datetime", "description", "icon"]].to_csv(filepath + "conditions.csv")


# -- Dag Creation -- #
with DAG(
		"climatic_data",
		start_date=pendulum.datetime(2023, 1, 4, tz="UTC"),
		schedule_interval="0 0 * * 1"  # Execute every monday 00h00min
) as dag:
	task1 = BashOperator(
		task_id="create_directory",
		bash_command='mkdir -p "./week={{data_interval_end.strftime("%Y-%m-%d")}}"'
	)

	task2 = PythonOperator(
		task_id="extract_data",
		python_callable=extract_data,
		op_kwargs={"data_interval_end":'{{data_interval_end.strftime("%Y-%m-%d")}}'}
	)

	task1 >> task2
