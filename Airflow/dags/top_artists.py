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

def download_top_artists():
    lastfm_api_key = Variable.get('lastapi')
    print("Calling LAST.FM API to get top artists...")
    artists_url = f'https://ws.audioscrobbler.com/2.0/?method=chart.gettopartists&api_key={lastfm_api_key}&format=json&limit=1000'

    response = requests.get(artists_url)
    data = response.json()
    top_artists = [{key: artist[key] for key in ['name', 'listeners', 'playcount']} for artist in data['artists']['artist']]
    print(f"Retrieved {len(top_artists)} top artists from LAST.FM API.")
    top_artists_df = pd.DataFrame(top_artists)

    top_artists_df.to_csv(os.path.join(BASE_DIR, 'top_artists.csv'), index=False)


def ingest_top_artists():

    top_artists_local_path = os.path.join(BASE_DIR, 'top_artists.csv')
    container_name = Variable.get('container_name')
    blob_name = 'top_artists.csv'
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
    print(f"Uploading {top_artists_local_path} to Azure Blob Storage...")
    with open(top_artists_local_path, 'rb') as data:
        blob_client.upload_blob(data, overwrite=True)


with DAG('top_artists_dag', default_args=default_args, catchup=False) as dag:
    download_task = PythonOperator(
        task_id='download_top_artists',
        python_callable=download_top_artists
    )

    ingest_task = PythonOperator(
        task_id='ingest_top_artists',
        python_callable=ingest_top_artists
    )

    download_task >> ingest_task