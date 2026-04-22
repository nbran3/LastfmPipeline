from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime

with DAG('controller_dag', start_date=datetime(2026, 1, 1), catchup=False) as dag:
    trigger_top_artists = TriggerDagRunOperator(
        task_id='trigger_top_artists_dag',
        trigger_dag_id='top_artists_dag',
        wait_for_completion=True,
        poke_interval=30
    )

    trigger_artist_genres = TriggerDagRunOperator(
        task_id='trigger_artist_genres_dag',
        trigger_dag_id='artist_genres_dag',
        wait_for_completion=True,
        poke_interval=30
    )

    trigger_artist_tracks = TriggerDagRunOperator(
        task_id='trigger_artist_tracks_dag',
        trigger_dag_id='artist_tracks_dag',
        wait_for_completion=True,
        poke_interval=30
    )

    trigger_artist_albums = TriggerDagRunOperator(
        task_id='trigger_artist_albums_dag',
        trigger_dag_id='artist_albums_dag',
        wait_for_completion=True,
        poke_interval=30
    )

    trigger_geo_artists = TriggerDagRunOperator(
        task_id='trigger_geo_artist_dag',
        trigger_dag_id='geo_top_artists_dag',
        wait_for_completion=True,
        poke_interval=30
    )

    trigger_ingest_to_sql = TriggerDagRunOperator(
        task_id='trigger_ingest_to_sql_dag',
        trigger_dag_id='ingest_to_sql_dag',
        wait_for_completion=True,
        poke_interval=30
    )

    trigger_sqlmesh = TriggerDagRunOperator(
    task_id='run_sqlmesh',
    trigger_dag_id='sqlmesh_dag',
    wait_for_completion=True,
    poke_interval=30
)

    trigger_top_artists >>  trigger_artist_genres >> trigger_artist_tracks >> trigger_artist_albums >> trigger_geo_artists >> trigger_ingest_to_sql >> trigger_sqlmesh