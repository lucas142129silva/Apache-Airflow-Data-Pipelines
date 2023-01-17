"""This py file creates a connection via code"""
from airflow import settings
from airflow.models import Connection
import os

conn = Connection(
	conn_id="twitter_default",
	conn_type="HTTP",
	host="https://api.twitter.com",
	extra={"Authorization": os.environ.get("BEARER_TOKEN")}
)

session = settings.Session()
session.add(conn)
session.commit()