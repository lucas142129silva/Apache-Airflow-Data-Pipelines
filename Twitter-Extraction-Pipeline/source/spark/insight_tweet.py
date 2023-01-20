"""Transform silver data into gold data"""
from pyspark.sql import functions as f
from pyspark.sql import SparkSession
from os.path import join
import argparse


def get_tweet_talk(df_tweet):
	"""Get the information of interactions from the tweet"""
	tweet_talk = df_tweet.alias("tweet") \
		.groupBy(f.to_date("created_at").alias("created_date")) \
		.agg(
		f.countDistinct("author_id").alias("n_tweets"),
		f.sum("like_count").alias("n_likes"),
		f.sum("quote_count").alias("n_quote"),
		f.sum("reply_count").alias("n_reply"),
		f.sum("retweet_count").alias("n_retweet")
	).withColumn("weekday", f.date_format("created_date", "E"))
	return tweet_talk


def export_json(df, destiny):
	df.coalesce(1).write.mode("overwrite").json(destiny)


def twitter_insight(spark_s, src, destiny, process_date):
	df_tweet = spark_s.read.json(join(src, "tweet"))
	tweet_talk = get_tweet_talk(df_tweet)
	export_json(tweet_talk, join(destiny, f"process_date={process_date}"))


if __name__ == "__main__":
	parser = argparse.ArgumentParser(description="Spark Twitter Transformation")
	parser.add_argument("--src", required=True)
	parser.add_argument("--destiny", required=True)
	parser.add_argument("--process-date", required=True)

	args = parser.parse_args()

	spark = SparkSession.builder.appName("twitter_transformation").getOrCreate()
	twitter_insight(spark_s=spark, src=args.src, destiny=args.destiny, process_date=args.process_date)