import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek


# This parses dl.cfg file which is then used to retrieve Access and Secret key of AWS's IAM role

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    
    '''
    
    Creates and returns SparkSession which is the entry point for Spark application
    
    '''
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    
    '''
    
    Receives the Spark session, and path to S3 bucket for input and output data
    
    JSON files are read from song_data, and then song & artist tables area created
    
    Lastly, both table are written to output data's S3 bucket in their respective directories    
        
    '''
    
    # Get filepath to song data file
    # To decrease time spent reading all the files in song_data, only '/A/A/A/*.json' is used. Otherwise, /*/*/*/*.json should be used
    song_data = os.path.join(input_data,"song_data/A/A/A/*.json")  

    # read song data file
    df = spark.read.json(song_data)
    df.show(5)

    # Extract columns to create songs table
    ## Attributues: song_id, title, artist_id, year, duration
    songs_table = df.dropDuplicates(['song_id']).select(['song_id','title','artist_id','year','duration'])
    #songs_table.show(3)
    
    # Write songs table to parquet files partitioned by year and artist
    songs_folder = os.path.join(output_data,'song_data/songs_table')
    songs_table.write.parquet(songs_folder, 'overwrite', ('year','artist_id'))

    # Extract columns to create artists table
    ## Attributues: artist_id, name, location, lattitude, longitude
    artists_table = df.dropDuplicates(['artist_id']).select(["artist_id","artist_name",'artist_location','artist_latitude','artist_longitude'])
    artists_table.show(3)
    
    # Write artists table to parquet files
    artists_folder = os.path.join(output_data, "song_data/artists_table")
    
    artists_table.write.parquet(artists_folder, 'overwrite')

def process_log_data(spark, input_data, output_data):
    
    '''
    
    Receives the Spark session, and path to S3 bucket for input and output data
    
    JSON files are read from song_data, and then user, time & songplays tables are created
    
    Lastly, all three tables are written to output data's S3 bucket in their respective directories    
        
    '''
    
    # Get filepath to log data file
    log_data = os.path.join(input_data, 'log-data/*/*/*.json')

    # Read log data file
    df = spark.read.json(log_data)
    
    # Filter by actions for song plays
    df = df.filter(df.page=='NextSong')

    # Extract columns for users table   
    ## Attributues:  user_id, first_name, last_name, gender, level
    users_table = df.dropDuplicates(['userId']).select(\
                    col('userId').alias('user_id'),
                    col('firstName').alias('first_name'),
                    col('lastName').alias('last_name'),
                    'gender',
                    'level'
                    )
    
    # Write users table to parquet files
    users_folder = os.path.join(output_data, 'log_data/users_table')
    users_table.write.parquet(users_folder, 'overwrite')

    # Create timestamp column from original timestamp column
    get_timestamp = udf(lambda x : datetime.fromtimestamp(x/1000),TimestampType())
    df = df.withColumn('timestamp',get_timestamp(df.ts))
    df.show(3)
    
    # Create datetime column from original timestamp column
    # get_datetime = udf() -- Since get_timestamp is able to provide date info in addition to timestamp, this will not be used
    # df = 
    
    # Extract columns to create time table
    ## Attributues: start_time, hour, day, week, month, year, weekday
    
    time_table = df.dropDuplicates(['timestamp']).select(\
                    col('timestamp').alias('start_time'),
                    hour('timestamp').alias('hour'),
                    dayofmonth('timestamp').alias('day'),
                    month('timestamp').alias('month'),
                    year('timestamp').alias('year'),
                    dayofweek('timestamp').alias('day_of_week')
                    )
    
    # Write time table to parquet files partitioned by year and month
    time_folder = os.path.join(output_data, 'log_data/time_table')
    time_table.write.parquet(time_folder, 'overwrite', ('year','month'))

    # Read in song data to use for songplays table
    song_data = os.path.join(input_data,"song_data/A/A/A/*.json")
    song_df= spark.read.json(song_data)

    # Extract columns from joined song and log datasets to create songplays table 
    ## Attributues: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
    
    songplays_table = df.join(song_df, (df.artist == song_df.artist_name) &\
                        (df.length == song_df.duration) &\
                        (df.song == song_df.title), 'left_outer')
        
    songplays_table = songplays_table.select(\
                        col("userId").alias("user_id"),
                        'level',
                        'song_id',
                        'artist_id',
                        col("sessionId").alias("session_id"),
                        'location',
                        col("userAgent").alias("user_agent"),
                        year('timestamp').alias('year'),
                        month('timestamp').alias('month')
                        )
    
    # Write songplays table to parquet files partitioned by year and month
    songplays_folder = os.path.join(output_data,'log_data/songplays')
    songplays_table.write.parquet(songplays_folder,'overwrite',('year','month'))

def main():
    
    '''
    Calls all other functions
    '''
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://asadsdatalake/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
