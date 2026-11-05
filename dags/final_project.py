from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from operators import (StageToRedshiftOperator, LoadFactOperator,
                       LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

default_args = {
    'owner': 'udacity',
    'start_date': pendulum.datetime(2018, 11, 1, tz='UTC'),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
)
def final_project():

    start_operator = DummyOperator(task_id='start_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        table='staging_events',
        s3_bucket='sparkify-airflow-787569976096-1793737086',
        s3_key="log-data/{{ logical_date.strftime('%Y/%m/%Y-%m-%d') }}-events.json",
        region='us-west-2',
        json_format=(
            's3://sparkify-airflow-787569976096-1793737086/'
            'log_json_path.json'
        ),
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        table='staging_songs',
        s3_bucket='sparkify-airflow-787569976096-1793737086',
        s3_key='song-data/A/A/',
        region='us-west-2',
        json_format='auto',
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id='redshift',
        table='songplays',
        sql=SqlQueries.songplay_table_insert,
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id='redshift',
        table='users',
        sql=SqlQueries.user_table_insert,
        truncate=True,
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id='redshift',
        table='songs',
        sql=SqlQueries.song_table_insert,
        truncate=True,
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id='redshift',
        table='artists',
        sql=SqlQueries.artist_table_insert,
        truncate=True,
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id='redshift',
        table='time',
        sql=SqlQueries.time_table_insert,
        truncate=True,
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id='redshift',
        checks=[
            {
                'sql': f"""
                    SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END
                    FROM {table}
                """,
                'expected_result': 1,
            }
            for table in ['songplays', 'users', 'songs', 'artists', 'time']
        ] + [
            {
                'sql': f"""
                    SELECT COUNT(*)
                    FROM {table}
                    WHERE {key} IS NULL
                """,
                'expected_result': 0,
            }
            for table, key in [
                ('songplays', 'playid'),
                ('users', 'userid'),
                ('songs', 'songid'),
                ('artists', 'artistid'),
                ('time', 'start_time'),
            ]
        ] + [
            {
                'sql': f"""
                    SELECT COUNT(*)
                    FROM (
                        SELECT {key}
                        FROM {table}
                        GROUP BY {key}
                        HAVING COUNT(*) > 1
                    ) duplicates
                """,
                'expected_result': 0,
            }
            for table, key in [
                ('songplays', 'playid'),
                ('users', 'userid'),
                ('songs', 'songid'),
                ('artists', 'artistid'),
                ('time', 'start_time'),
            ]
        ],
    )

    end_operator = DummyOperator(task_id='end_execution')

    _ = start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]

    _ = [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table

    _ = load_songplays_table >> [
        load_user_dimension_table,
        load_song_dimension_table,
        load_artist_dimension_table,
        load_time_dimension_table,
    ] >> run_quality_checks

    _ = run_quality_checks >> end_operator

final_project_dag = final_project()
