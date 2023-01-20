"""This py file is a first test of receiving tweets from a request on Twitter API and prints the output on screen"""
from datetime import datetime, timedelta
import os
import requests
import json

# -- Interval time for the query -- #
TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"
end_time = datetime.now().strftime(TIMESTAMP_FORMAT)
start_time = (datetime.now() + timedelta(-1)).date().strftime(TIMESTAMP_FORMAT)

# -- Search tweets with the query word -- #
query = "data science"

# -- Fields Requested -- #
tweet_fields = "tweet.fields=author_id,conversation_id,created_at,id,in_reply_to_user_id,public_metrics,lang,text"
user_fields = "expansions=author_id&user.fields=id,name,username,created_at"

# -- Query URL -- #
url_raw = "https://api.twitter.com/2/tweets/search/recent?query={}&{}&{}&start_time={}&end_time={}" \
	.format(query, tweet_fields, user_fields, start_time, end_time)

bearer_token = os.environ.get("BEARER_TOKEN")
headers = {"Authorization": "Bearer {}".format(bearer_token)}
response = requests.request("GET", url_raw, headers=headers)

# -- Twitter Return as JSON -- #
json_response = response.json()
print(json.dumps(json_response, indent=4, sort_keys=True))

# -- While there are pages to search - Iteration for all pages -- #
while "next_token" in json_response.get("meta", {}):
	next_token = json_response["meta"]["next_token"]
	url = f"{url_raw}&next_token={next_token}"
	response = requests.request("GET", url, headers=headers)
	json_response = response.json()
	print(json.dumps(json_response, indent=4, sort_keys=True))