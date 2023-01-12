import os
from os.path import join
import pandas as pd
from datetime import datetime, timedelta

# Data Interval
start_date = datetime.today()
end_date = start_date + timedelta(days=7)

# Processing the dates
start_date = start_date.strftime("%Y-%m-%d")
end_date = end_date.strftime("%Y-%m-%d")

city = "Boston"
key = "5WE9M7VXYCBTRPXV388K2JGPR"

URL = join("https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/",
            f"{city}/{start_date}/{end_date}?unitGroup=metric&include=days&key={key}&contentType=csv")

data = pd.read_csv(URL)

filepath = f"./week={start_date}/"
os.mkdir(filepath)

data.to_csv(filepath + "raw_data.csv")
data[["datetime", "tempmin", "temp", "tempmax"]].to_csv(filepath + "temperatures.csv")
data[["datetime", "description", "icon"]].to_csv(filepath + "conditions.csv")