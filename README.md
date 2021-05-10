# Project: Data Lake

## Background

Note: This project builds onto the client, Sparkify's evolving requirements from project 1, 2 and 3.

In the first 2 projects, Sparkify needed Postgres/Cassandra database with ETL pipeline to process locally-hosted data, transform and load it into the databases. The data included user activity, and metadata of the songs in their app. The data was provided in JSON.

Then Sparkify's requirement evolved to requiring staging tables which their Analytics team would use to find insights. This included data being located on Amazon S3 from which ETL pipeline would be directed to Amazon Redshift. Star schema was used for staging the tables, to enhance usability for the Analytics team.

While Sparkify's overarching requirement remains the same, of being able to find insights in what songs their users are listening to, this project addresses their growing user and songs database by leveraging Data Lake. 

**For this project**, the Data Engineering team is tasked with building ETL pipeline to retrieve the data from Amazon S3, process it using Apache Spark and place it back in S3, only this time the 'processed' data will be organized into a set of dimensional tables and loaded back into S3.

Although project requirement states that Spark needs to run on an AWS EMR cluster, refer to section 'Curiosity Questions' for the actual approach taken.

Data Engineering team will be conducting 3 tasks: 

1. Access Sparkify's public S3 bucket to retrieve user activity and songs data
2. Using Spark (PySpark), data is extracted and transformed according to each dimensional table's attributes
3. Tables are written back to a seperate bucket, into their individual directories as parquet files, partitioned as needed.

## Data Lake Design 

As per task # 3, tables will be placed in seperate directories (and partitioned as needed).

**Before describing the tables, the datsets for user activity and songs metadata are described:**

### Staging table's attributes/columns, data types are as follows:

Name of dataset: log_data
Attribute/Data-Type/Constraints (if applicable):

- artist: string
- auth: string
- firstName: string
- gender: string
- itemInSession: long
- lastName: string
- length: double
- level: string
- location: string
- method: string
- page: string 
- registration: double 
- sessionId: long 
- song: string
- status: long 
- ts: long
- userAgent: string
- userId: string 

Name of table: song_data
Attribute/Data-Type/Constraints (if applicable):

- artist_id: string
- artist_latitude: double 
- artist_location: string 
- artist_longitude: double
- artist_name: string
- duration: double
- num_songs: long
- song_id: string
- title: string
- year: long

### Dimensional table attributes/columns, data types and constraints are as follows:

Name of table: songs
Attribute/Data-Type/Constraints (if applicable):
Partitioned by (\*): year & artist_id
Distinct attribute: song_id

- song_id: string
- title: string
- artist_id: string \*
- year: long \*
- duration: double 

Name of table: artists
Attribute/Data-Type/Constraints (if applicable):
Partitioned by: None
Distinct attribute: artist_id

- artist_id: string
- artist_name: string
- artist_location: string 
- artist_latitude: double 
- artist_longitude: double

Name of table: users
Attribute/Data-Type/Constraints (if applicable):
Partitioned by: None
Distinct attribute: user_id

- user_id string 
- first_name string 
- last_name string 
- gender string 
- level string 

Name of table: time
Attribute/Data-Type/Constraints (if applicable):
Partitioned by (\*): year & month
Distinct attribute: timestamp

- start_time: timestamp
- date: string
- hour: integer 
- day: integer 
- week: integer 
- month: integer \*
- year: integer \*
- day_of_week: integer 

Name of table: songplays
Attribute/Data-Type/Constraints (if applicable):
Partitioned by (\*): year & month

- user_id: string
- level: string
- song_id: string 
- artist_id: string 
- session_id: long 
- location: string
- user_agent: string
- year: integer \*
- month: integer \*

## How to run the scripts

To run this project:

*It is assumed that IAM role have been created, and Access and Secret key are listed in dl.cfg.

Apache Spark was not used on AWS EMR. *It was used locally.

1. Double check that all of the folders for song_data and log_data are being accessed by using '\*' for each directory.
1. Run etl.py in terminal using 'python etl.py'

## Curiosity question
 *Purpose of this section is to list questions that occurred to me while doing this project but I was unable to find answers for them
 
1. Referring to values in .config of function 'spark', are they referring to Spark being hosted locally? 
- I was able to locally-run Spark for the entirety of the project but faced the prolonged time duration while all files from both datasets were read.

2. What additional steps would be needed if Spark was run on EMR cluster?
- I recall being about to create Jupyter Notebook within EMR but I haven't quite understood how to upload files from local machine.

3. How to access Spark's UI if hosted locally?
- I tried various ports (i.e. 4040, 4041, 8080) while running Spark on Udacity's workspace but was unsuccessful in access the UI.

4. What mode of join is correct for producing songplays table.
- Although I used 'left_outer' for joining both datasets, I found *null* values for most records' values associated to songs_data's attributes