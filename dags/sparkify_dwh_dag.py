import datetime

from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
# Custom Operators
from operators.stage_redshift import StageToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import DataQualityOperator

from helpers import SqlQueries

default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': datetime.timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

@dag(start_date=datetime.datetime(2018, 11, 1, 0, 0, 0, 0),
    end_date=datetime.datetime(2019, 11, 30, 0, 0, 0, 0),
    default_args=default_args,
    schedule_interval='@daily',
    max_active_runs=1,
    )
def sparkify_dwh_dag():
    
    start_operator = DummyOperator(task_id='begin_execution')
    
    create_tables = PostgresOperator(
        task_id="create_tables",
        postgres_conn_id="redshift",
        sql=SqlQueries.create_tables
    )
    
    stage_events_to_redshift = StageToRedshiftOperator(
        task_id="stage_events",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table_name="staging_events",
        s3_bucket="udacity-dend",
        s3_key="log_data/{data_interval_start.year}/{data_interval_start.month}/{ds}-events.json",
        json_path="log_json_path.json",
        aws_region="us-west-2",
        ignore_headers=1,
    )
    
    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id="stage_songs",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table_name="staging_songs",
        s3_bucket="udacity-dend",
        s3_key="song_data",
        aws_region="us-west-2",
        ignore_headers=1,
    )
    
    load_songplays_table = LoadFactOperator(
        task_id="load_songplays_fact_table",
        redshift_conn_id="redshift",
        table_name="songplays",
        sql=SqlQueries.songplay_table_insert
    )
    
    load_user_dimension_table = LoadDimensionOperator(
        task_id="load_user_dim_table",
        redshift_conn_id="redshift",
        table_name="users",
        sql=SqlQueries.user_table_insert,
        append_only=False,
    )
    
    load_song_dimension_table = LoadDimensionOperator(
        task_id="load_song_dim_table",
        redshift_conn_id="redshift",
        table_name="songs",
        sql=SqlQueries.song_table_insert,
        append_only=False,
    )
    
    load_artist_dimension_table = LoadDimensionOperator(
        task_id="load_artist_dim_table",
        redshift_conn_id="redshift",
        table_name="artists",
        sql=SqlQueries.artist_table_insert,
        append_only=False,
    )
    
    load_time_dimension_table = LoadDimensionOperator(
        task_id="load_time_dim_table",
        redshift_conn_id="redshift",
        table_name="time",
        sql=SqlQueries.time_table_insert,
        append_only=False,
    )
    
    run_data_quality_checks = DataQualityOperator(task_id="run_data_quality_checks",
                                                  redshift_conn_id="redshift",
                                                  staging_table_names=["staging_events", "staging_songs"],
                                                  table_column_map={"songplays": "playid",
                                                                    "users": "userid",
                                                                    "songs": "songid",
                                                                    "artists": "artistid"}
    )
    
    
    end_operator = DummyOperator(task_id='stop_execution')
    
    start_operator >> create_tables
    
    create_tables >> [stage_events_to_redshift, stage_songs_to_redshift]
    
    [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
    
    load_songplays_table >> [load_song_dimension_table,
                             load_user_dimension_table,
                             load_artist_dimension_table,
                             load_time_dimension_table]
    
    [load_song_dimension_table,
     load_user_dimension_table,
     load_artist_dimension_table,
     load_time_dimension_table] >> run_data_quality_checks
    
    run_data_quality_checks >> end_operator
    
sparkify_dwh_dag()
    