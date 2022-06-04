# Project: Data Lake
## Introduction and Project Description
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

This project provides an ETL pipeline that extracts their data from S3, processes them with Spark, and loads the data back into S3 as a set of dimensional tables.
This will allow their analytics team to further dig into insights into what songs their users are listening to.

## Project Datasets
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

## How to run
- Add your IAM role (AWS credentials) info to `dl.cfg`

- Execute the ETL pipeline by using command line: `python etl.py`


## OPTIONAL: Question for the reviewer
 
If you have any question about the starter code or your own implementation, please add it in the cell below. 

For example, if you want to know why a piece of code is written the way it is, or its function, or alternative ways of implementing the same functionality, or if you want to get feedback on a specific part of your code or get feedback on things you tried but did not work.

Please keep your questions succinct and clear to help the reviewer answer them satisfactorily. 

> **_Your question_**