import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = f"{input_data}*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select("song_id","title","artist_id","year","duration").drop_duplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year","artist_id").parquet(f"{output_data}songs_table.parquet", "overwrite")

    # extract columns to create artists table
    artists_table = df.select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude").distinct()
    
    # write artists table to parquet files
    artists_table.write.parquet(f"{output_data}artist_table.parquet", "overwrite")


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = f"{input_data}log_data/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table    
    artists_table = df.select("userId","firstName","lastName","gender","level").drop_duplicates()
    
    # write users table to parquet files
    artists_table.write.parquet(f"{output_data}users_table.parquet", "overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000.0), TimestampType())
    df = df.withColumn("start_time", get_timestamp(col('ts')))
    
    # extract columns to create time table
    time_table = df.select("start_time")\
                .withColumn("hour", hour(df.start_time))\
                .withColumn("day", dayofmonth(df.start_time))\
                .withColumn("week", weekofyear(df.start_time))\
                .withColumn("month", month(df.start_time))\
                .withColumn("year", year(df.start_time))\
                .withColumn("weekday", dayofweek(df.start_time)).drop_duplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(f"{output_data}times_table.parquet", "overwrite")

    # read in song data to use for songplays table
    song_df = spark.read.parquet(f"{output_data}songs_table.parquet")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, (df.song == song_df.title) & (df.length == song_df.duration), 'left_outer').withColumn("songplay_id", monotonically_increasing_id()).drop_duplicates()
    
    songplays_table = songplays_table.selectExpr("songplay_id","start_time","userId as user_id","level","song_id","artist_id","sessionId as session_id","location","userAgent as user_agent")

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(f"{output_data}songplays_table.parquet", "overwrite")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://datalakesudacityproj/output_data/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
