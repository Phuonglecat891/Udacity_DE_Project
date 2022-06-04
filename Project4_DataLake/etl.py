import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Date, TimestampType as Timestamp


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['KEYS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['KEYS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    '''
    Create spark session
    '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''
    Read song_data from s3, processes it into songs, artists tables
    Then writes the tables into parquet files on S3.
    
    input:
        spark: spark session
        input_data: s3 input file path
        output_data: s3 output file path
    '''
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    song_data_schema = R([
        Fld("artist_id",Str()),
        Fld("artist_latitude",Dbl()),
        Fld("artist_location",Str()),
        Fld("artist_longitude",Dbl()),
        Fld("artist_name",Str()),
        Fld("duration",Dbl()),
        Fld("num_songs",Int()),
        Fld("song_id",Str()),
        Fld("title",Str()),
        Fld("year",Int()),
    ])
    df = spark.read.json(song_data, schema=song_data_schema)
    df = df.dropDuplicates()
    
    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration').dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data + "songs_table", mode="overwrite", partitionBy=["year", "artist_id"])

    # extract columns to create artists table
    artists_table = df.select('artist_id', col("artist_name").alias("name"), col("artist_location").alias("location"), col("artist_latitude").alias("latitude"), col("artist_longitude").alias("longitude")).dropDuplicates() 
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists_table", mode="overwrite")


def process_log_data(spark, input_data, output_data):
    '''
    Read log_data from s3, processes it into users, songplays tables
    Then writes the tables into parquet files on S3.
    
    input:
        spark: spark session
        input_data: s3 input file path
        output_data: s3 output file path
    '''
    
    # get filepath to log data and song_data file
    log_data = input_data + 'log_data/*/*/*.json'
    song_data = input_data + 'song_data/*/*/*/*.json'

    # read log data file
    log_data_schema = R([
        Fld("artist",Str()),
        Fld("auth",Str()),
        Fld("firstName",Str()),
        Fld("gender",Str()),
        Fld("itemInSession",Int()),
        Fld("lastName",Str()),
        Fld("length",Dbl()),
        Fld("level",Str()),
        Fld("location",Str()),
        Fld("method",Str()),
        Fld("page",Str()),
        Fld("registration",Dbl()),
        Fld("sessionId",Int()),
        Fld("song",Str()),
        Fld("status",Int()),
        Fld("ts",Dbl()),
        Fld("userAgent",Str()),
        Fld("userId",Str())
    ])
    
    df = spark.read.json(log_data, schema=log_data_schema)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users = df.select(col('userId').alias('user_id'), 
                      col('firstName').alias('first_name'), 
                      col('lastName').alias('last_name'), 
                      'gender', 
                      'level').dropDuplicates()
    
    # write users table to parquet files
    users.write.parquet(output_data + "users_table", mode="overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x // 1000.0), Timestamp())
    df = df.withColumn("timestamp",get_timestamp(df.ts))
    
    # extract columns to create time table
    time_table = df.select(col('timestamp').alias('start_time')).distinct()
    time_table = time_table.withColumn('hour', hour(time_table.start_time))
    time_table = time_table.withColumn('day', dayofmonth(time_table.start_time))
    time_table = time_table.withColumn('week', weekofyear(time_table.start_time))
    time_table = time_table.withColumn('month', month(time_table.start_time))
    time_table = time_table.withColumn('year', year(time_table.start_time))
    time_table = time_table.withColumn('weekday', dayofweek(time_table.start_time))         
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data + "time_table", mode="overwrite", partitionBy=["year", "month"])

        
    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + "songs_table")
    artist_df = spark.read.parquet(output_data + "artists_table")
    
    mix_df = (song_df.join(artist_df, 'artist_id', 'full')) \
             .select('song_id', 'title', 'artist_id', 'name', 'duration')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = (df.join(mix_df, (df.song == mix_df.title) & (df.artist == mix_df.name) & (df.length == mix_df.duration), 'left_outer')) \
                        .select(col('timestamp').alias('start_time'),
                                col('userId').alias('user_id'),
                                df.level,
                                mix_df.song_id,
                                mix_df.artist_id,
                                col('sessionId').alias('session_id'),
                                df.location,
                                col('userAgent').alias('user_agent')
                               )
    songplays_table = songplays_table.withColumn('year', year(songplays_table.start_time))
    songplays_table = songplays_table.withColumn('month', month(songplays_table.start_time)) 
    
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(output_data + "songplays_table", mode="overwrite", partitionBy=["year", "month"])

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://project4-mybucket/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)

if __name__ == "__main__":
    main()
