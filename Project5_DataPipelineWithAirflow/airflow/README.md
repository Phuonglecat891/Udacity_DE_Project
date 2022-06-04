# Overview
A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

This project builds a data pipeline using Apache Airflow to create a scheduler which automates and monitors the running of an ETL pipeline serving Sparkify service.

# File structure
∟dags
	∟main_etl_dag.py				define tasks and dependencies of the DAG.
∟plugins
	∟helpers
		∟sql_queries.py				contains the SQL queries used in the ETL process.
	∟operators
		∟data_quality.py			contains `DataQualityOperator`, which runs a data quality check by passing a list of SQL queries.
		∟load_dimension.py			contains `LoadDimensionOperator`, which loads a dimension table from staging tables.
		∟load_fact.py				contains `LoadFactOperator`, which loads a fact table from data in the staging tables.
		∟stage_redshift.py			contains `StageToRedshiftOperator`, which copies JSON data from S3 to staging tables on Redshift
∟sql 
	∟create_tables.sql
∟README.md

# How to run
Open Airfow UI
Add the following Airflow connections:
	AWS credentials
	Connection to Redshift cluster
Turn ON Dag