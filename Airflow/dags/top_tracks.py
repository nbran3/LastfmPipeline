import os
import requests
import pandas as pd
from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient
import json
from airflow import DAG
from airflow.sdk import Variable
from airflow.providers.standard.operators.python import PythonOperator

default_args = {
    'start_date': '2026-01-01',
    'retries': 3,
    'retry_delay': 300
}

BASE_DIR = os.path.join(os.path.dirname(__file__), 'data')


def download_top_tracks():
    lastfm_api_key = Variable.get('lastapi')

    print("Calling LAST.FM API to get top tracks...")
    url = f'https://ws.audioscrobbler.com/2.0/?method=chart.gettoptracks&api_key={lastfm_api_key}&format=json&limit=100'

    response = requests.get(url)
    data = response.json()
    top_tracks = [{key: track[key] for key in ['name', 'listeners', 'playcount']} for track in data['tracks']['track']]
    print(f"Retrieved {len(top_tracks)} top tracks from LAST.FM API.")
    top_tracks_df = pd.DataFrame(top_tracks)

    top_tracks_df.to_csv(os.path.join(BASE_DIR, 'top_tracks.csv'), index=False)


def ingest_top_tracks():

    top_tracks_local_path = os.path.join(BASE_DIR, 'top_tracks.csv')
    container_name = Variable.get('container_name')
    blob_name = 'top_tracks.csv'
    account_url = Variable.get('azure_account')
    creds = json.loads(Variable.get('properties'))

    credential = ClientSecretCredential(
        tenant_id=creds['tenantId'],
        client_id=creds['clientId'],
        client_secret=creds['client_secret']
    )

    blob_service_client = BlobServiceClient(account_url=account_url, credential=credential)
    container_client = blob_service_client.get_container_client(container_name)

    blob_client = container_client.get_blob_client(blob_name)
    print(f"Uploading {top_tracks_local_path} to Azure Blob Storage...")
    with open(top_tracks_local_path, 'rb') as data:
        blob_client.upload_blob(data, overwrite=True)

    os.remove(top_tracks_local_path)
    print(f"Deleted local file {top_tracks_local_path} after upload.")


with DAG('top_tracks_dag', default_args=default_args, catchup=False) as dag:
    download_task = PythonOperator(
        task_id='download_top_tracks',
        python_callable=download_top_tracks
    ) 
    ingest_task = PythonOperator(
        task_id='ingest_top_tracks',
        python_callable=ingest_top_tracks
    )
    download_task >> ingest_task