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

COUNTRIES = [
    'United States', 'United Kingdom', 'Germany', 'France', 'Japan',
    'Australia', 'Canada', 'Brazil', 'Mexico', 'South Korea',
    'Sweden', 'Norway', 'Nigeria', 'India', 'Argentina',
    'Spain', 'Italy', 'Netherlands'
]

def download_geo_top_artists():
    os.makedirs(BASE_DIR, exist_ok=True)
    lastfm_api_key = Variable.get('lastapi')

    print("Calling LAST.FM API to get top artists by country...")
    results = []
    for country in COUNTRIES:
        url = 'http://ws.audioscrobbler.com/2.0/'
        params = {
            'method': 'geo.gettopartists',
            'country': country,
            'api_key': lastfm_api_key,
            'format': 'json',
            'limit': 100
        }

        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        if 'error' in data:
            print(f"Warning: API error for country '{country}': {data.get('message')}")
            continue

        artists = data['topartists']['artist']
        for artist in artists:
            results.append({
                'country': country,
                'rank': artist['@attr']['rank'],
                'artist': artist['name'],
                'listeners': artist['listeners']
            })
            print(f"Country: {country}, Rank: {artist['@attr']['rank']}, Artist: {artist['name']}")

    geo_df = pd.DataFrame(results)
    geo_df.to_csv(os.path.join(BASE_DIR, 'geo_top_artists.csv'), index=False)
    print(f"Saved {len(geo_df)} rows to geo_top_artists.csv")

def ingest_geo_top_artists():
    geo_local_path = os.path.join(BASE_DIR, 'geo_top_artists.csv')
    container_name = Variable.get('container_name')
    blob_name = 'geo_top_artists.csv'
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
    print(f"Uploading {geo_local_path} to Azure Blob Storage...")
    with open(geo_local_path, 'rb') as data:
        blob_client.upload_blob(data, overwrite=True)

    os.remove(geo_local_path)
    print(f"Deleted local file {geo_local_path} after upload.")


with DAG('geo_top_artists_dag', default_args=default_args, catchup=False) as dag:
    download_task = PythonOperator(
        task_id='download_geo_top_artists',
        python_callable=download_geo_top_artists
    )

    ingest_task = PythonOperator(
        task_id='ingest_geo_top_artists',
        python_callable=ingest_geo_top_artists
    )

    download_task >> ingest_task
    