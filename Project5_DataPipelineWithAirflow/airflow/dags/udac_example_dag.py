from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator, PostgresOperator)
from helpers import SqlQueries

AWS_KEY = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          template_searchpath = '/home/workspace/airflow/sql'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_all_tables = PostgresOperator(
    task_id="create_all_tables",
    dag=dag,
    postgres_conn_id="redshift",
    sql='create_tables.sql',
    autocommit = True
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='public.staging_events',
    s3_bucket='udacity-dend',
    s3_key='log_data',
    json_format='s3://udacity-dend/log_json_path.json'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='public.staging_songs',
    s3_bucket='udacity-dend',
    s3_key='song_data'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift',
    table="public.songplays",
    select_query=SqlQueries.songplay_table_insert,
    truncate_mode=True
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table="public.users",
    select_query=SqlQueries.user_table_insert,
    truncate_mode=True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table="public.songs",
    select_query=SqlQueries.song_table_insert,
    truncate_mode=True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table="public.artists",
    select_query=SqlQueries.artist_table_insert,
    truncate_mode=True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table="public.time",
    select_query=SqlQueries.time_table_insert,
    truncate_mode=True
)

lst_validate_query = [{'validate_query': "SELECT COUNT(userid) FROM users WHERE userid IS NULL",
                       'expected_result': 0},
                      {'validate_query': "SELECT COUNT(songid) FROM songs WHERE songid IS NULL",
                       'expected_result': 0}]

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    lst_validate_query=lst_validate_query
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

## DAG GRAPH ##
start_operator >> create_all_tables
create_all_tables >> stage_songs_to_redshift
create_all_tables >> stage_events_to_redshift

stage_songs_to_redshift >> load_songplays_table
stage_events_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator
