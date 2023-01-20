"""Transform the bronze data into a silver data"""
from pyspark.sql import functions as f
from pyspark.sql import SparkSession
from os.path import join
import argparse


# -- Transform the column data into lines -- #
def get_tweets_data(df):
    tweet_df = df.select(f.explode("data").alias("tweets")).select("tweets.author_id", "tweets.conversation_id",
                                                                   "tweets.created_at", "tweets.id",
                                                                   "tweets.public_metrics.*", "tweets.text")
    return tweet_df


def get_user_data(df):
    user_df = df.select(f.explode("includes.users").alias("users")).select("users.*")
    return user_df


def export_json(df, target_path):
    df.coalesce(1).write.mode("overwrite").json(target_path)


def twitter_transformation(spark_s, src, destiny, process_date):
    df = spark_s.read.json(src)
    tweet_df = get_tweets_data(df)
    user_df = get_user_data(df)

    table_destiny = join(destiny, "{table_name}", f"process_date={process_date}")
    export_json(tweet_df, table_destiny.format(table_name="tweet"))
    export_json(user_df, table_destiny.format(table_name="user"))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Spark Twitter Transformation")
    parser.add_argument("--src", required=True)
    parser.add_argument("--destiny", required=True)
    parser.add_argument("--process-date", required=True)

    args = parser.parse_args()

    spark = SparkSession.builder.appName("twitter_transformation").getOrCreate()
    twitter_transformation(spark_s=spark, src=args.src, destiny=args.destiny, process_date=args.process_date)