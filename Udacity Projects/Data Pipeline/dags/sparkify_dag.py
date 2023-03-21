from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
    'catchup': False
}

dag = DAG('sparkify_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(
    task_id='Begin_execution',  
    dag=dag,
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table = "staging_events",
    aws_credentials = "aws_credentials",
    redshift_conn_id = "redshift",
    s3_bucket = "udacity-dend",
    s3_key= "log_data",
    file_type = "JSON",
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table = "staging_songs",
    aws_credentials = "aws_credentials",
    redshift_conn_id = "redshift",
    s3_bucket = "udacity-dend",
    s3_key= "log_data",
    file_type = "JSON",
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table = "songplays",
    sql_stmt = SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table = "users",
    sql_stmt = SqlQueries.user_table_insert,
    truncate = True
)


load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table = "songs",
    sql_stmt = SqlQueries.song_table_insert,
    truncate = True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table = "artists",
    sql_stmt = SqlQueries.artist_table_insert,
    truncate = True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table = "time",
    sql_stmt = SqlQueries.time_table_insert,
    truncate = True
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=['songplays', 'users', 'songs', 'artists', 'time']
)

end_operator = DummyOperator(
    task_id='Stop_execution',  
    dag=dag
)
