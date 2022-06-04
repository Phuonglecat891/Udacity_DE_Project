# Project: Data Modeling with Postgres

## Project Description
A startup called Sparkify has collected songs and user activity on their new music streaming app.  All the above data is stored in json files and this makes it difficult to query and analyze.

The goal of the project is to define fact and dimension tables for a star schema for a particular analytic focus, and write an ETL pipeline that transfers data from files in two local directories into these tables in Postgres using Python and SQL.

## Dataset
The collected JSON data is stored in two directories data/log_data and data/song_data

### song_data format
```json
{
    "num_songs":1
    "artist_id":"ARD7TVE1187B99BFB1"
    "artist_latitude":null
    "artist_longitude":null
    "artist_location":"California - LA"
    "artist_name":"Casual"
    "song_id":"SOMZWCG12A8C13C480"
    "title":"I Didn't Mean To"
    "duration":218.93179
    "year":0
}
```

### log_data format
```json
{
    "artist":null,
    "auth":"Logged In",
    "firstName":"Walter",
    "gender":"M",
    "itemInSession":0,
    "lastName":"Frye",
    "length":null,
    "level":"free",
    "location":"San Francisco-Oakland-Hayward, CA",
    "method":"GET",
    "page":"Home",
    "registration":1540919166796.0,
    "sessionId":38,
    "song":null,
    "status":200,
    "ts":1541105830796,
    "userAgent":"\"Mozilla\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"",
    "userId":"39"
}
```

## Database schema
### Fact Table
#### songplays
Records in log data associated with song plays i.e. records with page NextSong
```sql
songplays (
        songplay_id varchar NOT NULL, 
        start_time timestamp without time zone NOT NULL, 
        user_id int NOT NULL, 
        level varchar NOT NULL, 
        song_id varchar, 
        artist_id varchar, 
        session_id int NOT NULL, 
        location varchar NOT NULL,
        user_agent varchar NOT NULL
);
```

### Dimension Tables
#### users 
```sql
users (
        user_id int PRIMARY KEY, 
        first_name varchar NOT NULL, 
        last_name varchar NOT NULL, 
        gender varchar NOT NULL, 
        level varchar NOT NULL 
);
```
#### songs 
```sql
songs (
        song_id varchar PRIMARY KEY, 
        title varchar NOT NULL, 
        artist_id varchar NOT NULL, 
        year int NOT NULL, 
        duration numeric NOT NULL
);
```
#### artists 
```sql
artists (
        artist_id varchar PRIMARY KEY, 
        name varchar NOT NULL, 
        location varchar, 
        latitude numeric NOT NULL, 
        longitude numeric
);
```
#### time 
```sql
time (
        start_time timestamp without time zone NOT NULL, 
        hour int NOT NULL, 
        day int NOT NULL, 
        week int NOT NULL, 
        month int NOT NULL, 
        year int NOT NULL, 
        weekday int NOT NULL
);
```

## ETL
Script python `elt.py`

Function `process_song_file` will extract data from song_data for the `artists` and `songs` tables.

Function `process_log_file` will extract data from log_data for the `time`,`users` and `songplays` tables.
This function will convert the timestamp value to datetime before inserting it into `time` table.
The data added to the `songplays` table is combined from the `artists` table, `song` tabel and the information in the log_data.

## Run Project
Using below command to create the database and tables:
`python create_tables.py`

After creating the database and tables we can start the ETL process:
`python etl.py`






