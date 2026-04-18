import requests
import pandas as pd
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import os

load_dotenv()

def download_top_tracks():
    lastfm_api_key = os.getenv('lastfm_api_key')

    url = f'https://ws.audioscrobbler.com/2.0/?method=chart.gettoptracks&api_key={lastfm_api_key}&format=json&limit=100'

    response = requests.get(url)
    data = response.json()
    top_tracks = [{key: track[key] for key in ['name', 'listeners', 'playcount']} for track in data['tracks']['track']]
    top_tracks_df = pd.DataFrame(top_tracks)

    top_tracks_df.to_csv('top_tracks.csv', index=False)


def ingest_top_tracks():

    top_tracks_local_path = './top_tracks.csv'
    container_name = os.getenv('container_name')
    blob_name = 'top_tracks.csv'
    account_url = os.getenv('Azure_account_url')
    blob_service_client = BlobServiceClient(account_url=account_url, credential=DefaultAzureCredential())
    container_client = blob_service_client.get_container_client(container_name)

    blob_client = container_client.get_blob_client(blob_name)
    with open(top_tracks_local_path, 'rb') as data:
        blob_client.upload_blob(data, overwrite=True)


if __name__ == "__main__":
    download_top_tracks()
    ingest_top_tracks()