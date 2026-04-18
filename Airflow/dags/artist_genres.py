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

def download_top_tags():
    lastfm_api_key = Variable.get('lastapi')
    artists_df = pd.read_csv(os.path.join(BASE_DIR, 'top_artists.csv'))

    print("Calling LAST.FM API to get top tags for each artist...")
    results = []
    for artist in artists_df['name']:
        url = 'http://ws.audioscrobbler.com/2.0/'
        params = {
        'method': 'artist.gettoptags',
        'artist': artist,
        'api_key': lastfm_api_key,
        'format': 'json'
        }
    
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        if 'error' in data:
            print(f"Warning: API error for artist '{artist}': {data.get('message')}")
            results.append({'artist': artist, 'genre': None})
            continue
    
        tags = data['toptags']['tag']
        top_tag = tags[0]['name'] if tags else None
        
        print(f"Artist: {artist}, Top Genre: {top_tag}")
        results.append({'artist': artist, 'genre': top_tag})

    genre_df = pd.DataFrame(results)
    genre_df.to_csv(os.path.join(BASE_DIR, 'artist_genres.csv'), index=False)

def ingest_top_genres():

    top_genres_local_path = os.path.join(BASE_DIR, 'artist_genres.csv')
    top_artists_local_path = os.path.join(BASE_DIR, 'top_artists.csv')
    container_name = Variable.get('container_name')
    blob_name = 'artist_genres.csv'
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
    print(f"Uploading {top_genres_local_path} to Azure Blob Storage...")
    with open(top_genres_local_path, 'rb') as data:
        blob_client.upload_blob(data, overwrite=True)
    
    os.remove(top_genres_local_path)
    os.remove(top_artists_local_path)
    print(f"Deleted local file {top_genres_local_path} and {top_artists_local_path} after upload.")


with DAG('artist_genres_dag', default_args=default_args, catchup=False) as dag:
    download_task = PythonOperator(
        task_id='download_top_tags',
        python_callable=download_top_tags
    )

    ingest_task = PythonOperator(
        task_id='ingest_top_genres',
        python_callable=ingest_top_genres
    )

    download_task >> ingest_task