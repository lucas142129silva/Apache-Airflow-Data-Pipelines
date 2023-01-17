# -- Creation of a Hook requesting data from Twitter API -- #
from airflow.providers.http.hooks.http import HttpHook
import requests

# -- JSON Manipulation -- #
import json

# -- Time and Date -- #
from datetime import datetime, timedelta


class TwitterHook(HttpHook):
	def __init__(self, end_time, start_time, query, conn_id=None):
		self.conn_id = conn_id or "twitter_default"
		super().__init__(http_conn_id=self.conn_id)

		# -- Url Building -- #
		self.end_time = end_time
		self.start_time = start_time
		self.query = query

	def create_url(self):
		"""Create a URL Query, using the time interval and term"""
		# -- Time Interval -- #
		end_time = self.end_time
		start_time = self.start_time

		# -- Search tweets with the query word -- #
		query = self.query

		# -- Fields Requested -- #
		tweet_fields = "tweet.fields=author_id,conversation_id,created_at,id,in_reply_to_user_id,public_metrics,lang,text"
		user_fields = "expansions=author_id&user.fields=id,name,username,created_at"

		# -- Query URL -- #
		url_raw = "{}/2/tweets/search/recent?query={}&{}&{}&start_time={}&end_time={}" \
			.format(self.base_url, query, tweet_fields, user_fields, start_time, end_time)

		return url_raw

	def connect_to_endpoint(self, url, session):
		"""Connect to the query url by a request, prepare it and generates a response"""
		request = requests.Request("GET", url)
		prep = session.prepare_request(request)
		self.log.info(f"URL: {url}")
		return self.run_and_check(session, prep, {})

	def paginate(self, url_raw, session):
		"""Get the response and iterates for the pages, adding the results in a list"""
		json_response_list = list()
		response = self.connect_to_endpoint(url_raw, session)
		limit_count = 1

		# -- Twitter Return as JSON -- #
		json_response = response.json()
		json_response_list.append(json_response)

		# -- While there are pages to search (limited to 100)- Iteration for all pages -- #
		while "next_token" in json_response.get("meta", {}) and limit_count < 100:
			next_token = json_response["meta"]["next_token"]
			url = f"{url_raw}&next_token={next_token}"
			response = self.connect_to_endpoint(url, session)
			json_response = response.json()
			json_response_list.append(json_response)
			limit_count += 1

		return json_response_list

	def run(self):
		session = self.get_conn()
		url_raw = self.create_url()

		return self.paginate(url_raw, session)


if __name__ == "__main__":
	# -- Personal Query -- #
	TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"
	end_time = datetime.now().strftime(TIMESTAMP_FORMAT)
	start_time = (datetime.now() + timedelta(-1)).date().strftime(TIMESTAMP_FORMAT)
	query = "data science"

	"""Paginates for pages returned and shows in the screen the json output as test"""
	for pg in TwitterHook(end_time, start_time, query).run():
		print(json.dumps(pg, indent=4, sort_keys=True))