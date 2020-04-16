import configparser
from datetime import datetime
import os
import boto3
import utils
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek, monotonically_increasing_id


# parse the config file
config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    This function creates the spark session.
    :return: spark object
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    This function reads the song data from the input s3 bucket 
    and then selects the relevant columns to store them in parquet 
    format in songs and artists table. It then writes to the output s3 bucket.
    :param spark:spark object
    :param input_data: path to the input s3 bucket
    :param output_data: path to the output s3 bucket
    :return: None
    """
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    print('Reading song data.')
    df = spark.read.json(song_data)
    print('Reading complete.')

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration').dropDuplicates()
    songs_table.createOrReplaceTempView('songs')
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(os.path.join(output_data, 'songs/songs.parquet'), 'overwrite') 
    print('Songs table is written to the parquet files.')

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude') \
                    .withColumnRenamed('artist_name', 'name') \
                    .withColumnRenamed('artist_location', 'location') \
                    .withColumnRenamed('artist_latitude', 'latitude') \
                    .withColumnRenamed('artist_longitude', 'longitude') \
                    .dropDuplicates()
    artists_table.createOrReplaceTempView('artists')
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists/artists.parquet'), 'overwrite')
    print('Artists table is written to the parquet files.')
    
    return


def process_log_data(spark, input_data, output_data):
    """
    This function reads the log data from the input s3 bucket 
    and then selects and preprocesses the relevant columns to store them in parquet 
    format in songplays, time, and users table. It then writes to the output s3 bucket.
    :param spark:spark object
    :param input_data: path to the input s3 bucket
    :param output_data: path to the output s3 bucket
    :return: None
    """
    # get filepath to log data file
    log_data = input_data + 'log_data/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df_actions = df.filter(df.page == 'NextSong').select('ts',
                                                        'userId',
                                                        'level',
                                                        'song',
                                                        'artist',
                                                        'sessionId',
                                                        'location',
                                                        'userAgent') 

    # extract columns for users table    
    users_table = df.select('userId', 'firstName', 'lastName', 'gender', 'level').dropDuplicates()
    users_table.createOrReplaceTempView('users')
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users/users.parquet'), 'overwrite')
    print('Users table is written to the parquet files.')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x)/1000)))
    df = df_actions.withColumn('timestamp', get_timestamp(df_actions.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x)/1000)))
    df_actions = df_actions.withColumn('datetime', get_datetime(df_actions.ts))

    
    # extract columns to create time table
    time_table = df_actions.select('datetime') \
                            .withColumn('start_time', df_actions.datetime) \
                            .withColumn('hour', hour('datetime')) \
                            .withColumn('day', dayofmonth('datetime')) \
                            .withColumn('week', weekofyear('datetime')) \
                            .withColumn('month', month('datetime')) \
                            .withColumn('year', year('datetime')) \
                            .withColumn('weekday', dayofweek('datetime')).dropDuplicates()
    
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, \
                                          'time/time.parquet'), 'overwrite')
    print('Time table is written to the parquet files.')

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + 'song_data/*/*/*/*.json ')

    # extract columns from joined song and log datasets to create songplays table 
    df_actions = df_actions.alias('log_df')
    df_song = song_df.alias('song_df')
    combined = df_actions.join(df_song, col('log_df.artist') == col('song_df.artist_name'),'inner')
    
    songplays_table = combined.select(col('log_df.datetime').alias('start_time'),
                                     col('log_df.userid').alias('user_id'),
                                     col('log_df.level').alias('level'),
                                     col('song_df.song_id').alias('song_id'),
                                     col('song_df.artist_id').alias('artist_id'),
                                     col('log_df.sessionId').alias('session_id'),
                                     col('log_df.location').alias('location'),
                                     col('log_df.userAgent').alias('user_agent'),
                                     year('log_df.datetime').alias('year'),
                                     month('log_df.datetime').alias('month')) \
                                    .withColumn('songplay_id', monotonically_increasing_id())

    songplays_table.createOrReplaceTempView('songplays')
    
    # write songplays table to parquet files partitioned by year and month
    time_table = time_table.alias('timetable')

    songplays_table.write.partitionBy(
        'year', 'month').parquet(os.path.join(output_data,
                                 'songplays/songplays.parquet'),
                                 'overwrite')
    print('Song_plays table is written to the parquet files.')
    
    return


def main():
    """
    This function performs the following:
    1. Creates a spark session
    2. Creates the putput S3 bucket
    3. Processes the song data
    4. Processes the log data
    :return: None
    """
    
    # creat the spark session
    spark = create_spark_session()
    print('Spark session has been created!')
    
    # read the input and output s3 bucket path
    input_data = config['S3']['S3_BUCKET_INPUT_PATH']
    output_data = config['S3']['S3_BUCKET_OUTPUT_PATH']
    print(output_data, output_data[6:-1], type(output_data))
    
    # create s3 output bucket
    s3 = boto3.client('s3')
    try:
        utils.create_bucket(s3, output_data[6:-1])
        print('Bucket is created')
    except:
        print('Please create the output s3 bucket.')
    
    # process the song data
    print('Song data is processing...!')
    process_song_data(spark, input_data, output_data)    
    print('Song data process is now complete.')
    
    # process the log data
    print('Log data is processing...!')
    process_log_data(spark, input_data, output_data)
    print('Log data process is now complete.')


if __name__ == "__main__":
    main()
