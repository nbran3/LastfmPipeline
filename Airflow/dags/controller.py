from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

with DAG('controller_dag', start_date=datetime(2026, 1, 1), catchup=False) as dag:
    trigger_top_artists = TriggerDagRunOperator(
        task_id='trigger_top_artists_dag',
        trigger_dag_id='top_artists_dag'
    )

    trigger_top_tracks = TriggerDagRunOperator(
        task_id='trigger_top_tracks_dag',
        trigger_dag_id='top_tracks_dag'
    )

    trigger_artist_genres = TriggerDagRunOperator(
        task_id='trigger_artist_genres_dag',
        trigger_dag_id='artist_genres_dag'
    )

    trigger_ingest_to_sql = TriggerDagRunOperator(
        task_id='trigger_ingest_to_sql_dag',
        trigger_dag_id='ingest_to_sql_dag'
    )

    trigger_top_artists >> trigger_top_tracks  >> trigger_artist_genres >> trigger_ingest_to_sql