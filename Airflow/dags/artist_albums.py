import os
import requests
import pandas as pd
from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient
import json
import time
from airflow import DAG
from airflow.sdk import Variable
from airflow.providers.standard.operators.python import PythonOperator

default_args = {
    'start_date': '2026-01-01',
    'retries': 3,
    'retry_delay': 300
}

BASE_DIR = os.path.join(os.path.dirname(__file__), 'data')

def download_top_artists_albums():
    lastfm_api_key = Variable.get('lastapi')
    artists_df = pd.read_csv(os.path.join(BASE_DIR, 'top_artists.csv'))

    print("Calling LAST.FM API to get top albums for each artist...")
    results = []
    for artist in artists_df['name']:
        url = 'http://ws.audioscrobbler.com/2.0/'
        params = {
        'method': 'artist.gettopalbums',
        'artist': artist,
        'api_key': lastfm_api_key,
        'format': 'json',
        'limit': 3
    }
    
        response = requests.get(url, params=params)
        data = response.json()
    
        if 'error' in data:
            print(f"Error fetching top albums for {artist}: {data['message']}")
            results.append({'artist': artist, 'album_name': None, 'top_album': None, 'playcount': None})
            continue

        albums = data['topalbums']['album']
    
        for album in albums:
            results.append({'artist': artist, 'album_name': album['name'], 'top_album': album.get('@attr', {}).get('rank'),  'playcount': album['playcount']})
            print(f"Fetched album '{album['name']}' for artist '{artist}' with playcount {album['playcount']}")
            time.sleep(.2)
    
    top_albums_df = pd.DataFrame(results)
    top_albums_df.to_csv(os.path.join(BASE_DIR, 'artist_albums.csv'), index=False)
    print(f"Saved top albums data to {os.path.join(BASE_DIR, 'artist_albums.csv')}")

def ingest_top_artists_albums():

    top_artists_albums_local_path = os.path.join(BASE_DIR, 'artist_albums.csv')
    top_artists_local_path = os.path.join(BASE_DIR, 'top_artists.csv')
    container_name = Variable.get('container_name')
    blob_name = 'artist_albums.csv'
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
    print(f"Uploading {top_artists_albums_local_path} to Azure Blob Storage...")
    with open(top_artists_albums_local_path, 'rb') as data:
        blob_client.upload_blob(data, overwrite=True)
    
    os.remove(top_artists_albums_local_path)
    os.remove(top_artists_local_path)
    print(f"Deleted local file {top_artists_albums_local_path} and {top_artists_local_path} after upload.")


with DAG('artist_albums_dag', default_args=default_args, catchup=False) as dag:
    download_task = PythonOperator(
        task_id='download_top_artists_albums',
        python_callable=download_top_artists_albums
    )

    ingest_task = PythonOperator(
        task_id='ingest_top_artists_albums',
        python_callable=ingest_top_artists_albums
    )
    
    download_task >> ingest_task