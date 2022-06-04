# Project Data Warehouse

## Overview
This project extracts data from AWS S3 to staging tables on Redshift, and transforms it into a set of dimensional tables for analytics team to continue finding insights in what songs their users are listening to.

Data stored in AWS S3 is set of file in JSON format as shown below

### Song Dataset in AWS S3
S3 link: s3://udacity-dend/song_data
A single song file looks like:
```
{"num_songs":       1, 
"artist_id":        "ARJIE2Y1187B994AB7", 
"artist_latitude":  null, 
"artist_longitude": null, 
"artist_location":  "", 
"artist_name":      "Line Renaud", 
"song_id":          "SOUPIRU12A6D4FA1E1", 
"title":            "Der Kleine Dompfaff", 
"duration":         152.92036, 
"year":             0}
```

### Log Dataset in AWS S3
S3 link: 3://udacity-dend/log_data
A single log file looks like:
```
{"artist":       null, 
"auth":          "Logged In", 
"firstName":     "Celeste", 
"gender":        "F", 
"itemInSession": 0, 
"lastName":      "Williams", 
"length":        null, 
"level":         "free", 
"location":      "Klamath Falls, OR", 
"method":        "GET",
"page":          "Home", 
"registration":  1.541078e+12, 
"sessionId":     438, 
"song":          null, 
"status":        200, 
"ts":            1541105830796, 
"userAgent":     "\"Mozilla\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"", 
"userId":        "53"}
```

## Tables in Redshift
### Staging tables
* staging_events
```
staging_events (
        artist varchar,
        auth varchar,
        first_name varchar,
        gender varchar,
        item_in_session integer,
        last_name varchar,
        length numeric,
        level varchar,
        location varchar,
        method varchar,
        page varchar,
        registration numeric,
        session_id int,
        song varchar,
        status integer,
        ts numeric,
        user_agent varchar,
        user_id int
)
```

* staging_songs
```
staging_songs (
        num_songs integer,
        artist_id varchar,
        artist_latitude numeric,
        artist_longitude numeric,
        artist_location varchar,
        artist_name varchar,
        song_id varchar,
        title varchar,
        duration numeric,
        year integer
)
```

### Fact table
* Records in event data associated with song plays i.e. records with page NextSong
```
songplays (
        songplay_id integer IDENTITY(0,1) PRIMARY KEY,
        start_time timestamp without time zone NOT NULL,
        user_id integer NOT NULL,
        level varchar NOT NULL,
        song_id varchar,
        artist_id varchar,
        session_id int NOT NULL,
        location varchar NOT NULL,
        user_agent varchar NOT NULL
)
```

### Dimension Tables
* Users - users in the app
```
users (
        user_id integer PRIMARY KEY, 
        first_name varchar NOT NULL, 
        last_name varchar NOT NULL, 
        gender varchar NOT NULL, 
        level varchar NOT NULL
)
```

* Songs - songs in music database
```
songs(
        song_id varchar PRIMARY KEY, 
        title varchar NOT NULL, 
        artist_id varchar NOT NULL, 
        year int NOT NULL, 
        duration numeric NOT NULL
)
```

* Artists - artists in music database
```
artists(
        artist_id varchar PRIMARY KEY, 
        name varchar NOT NULL, 
        location varchar, 
        latitude numeric NOT NULL, 
        longitude numeric
)
```

* Time - timestamps of records in songplays broken down into specific units
```
time(
        start_time timestamp without time zone PRIMARY KEY, 
        hour int NOT NULL, 
        day int NOT NULL, 
        week int NOT NULL, 
        month int NOT NULL, 
        year int NOT NULL, 
        weekday int NOT NULL
)
```

## ETL Pipeline
Connects to Redshift and access to S3.
Load data from S3 into staging tables on Redshift.
Process data from staging tables into analytics tables on Redshift.
Close the connection.

## How to run
- Add your Redshift database and IAM role info to `dwh.cfg`

- Run create_tables.py by using command line: `python create_tables.py`
Drop all table in Redshift
Create tables needed

- Run etl.py by using command line: `python etl.py`
