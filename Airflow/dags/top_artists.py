import requests
import pandas as pd
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import os

load_dotenv()

def download_top_artists():
    lastfm_api_key = os.getenv('lastfm_api_key')

    artists_url = f'https://ws.audioscrobbler.com/2.0/?method=chart.gettopartists&api_key={lastfm_api_key}&format=json&limit=100'

    response = requests.get(artists_url)
    data = response.json()
    top_artists = [{key: artist[key] for key in ['name', 'listeners', 'playcount']} for artist in data['artists']['artist']]
    top_artists_df = pd.DataFrame(top_artists)

    top_artists_df.to_csv('top_artists.csv', index=False)


def ingest_top_artists():

    top_artists_local_path = './top_artists.csv'
    container_name = os.getenv('container_name')
    blob_name = 'top_artists.csv'
    account_url = os.getenv('Azure_account_url')
    blob_service_client = BlobServiceClient(account_url=account_url, credential=DefaultAzureCredential())
    container_client = blob_service_client.get_container_client(container_name)

    blob_client = container_client.get_blob_client(blob_name)
    with open(top_artists_local_path, 'rb') as data:
        blob_client.upload_blob(data, overwrite=True)


if __name__ == "__main__":
    download_top_artists()
    ingest_top_artists()