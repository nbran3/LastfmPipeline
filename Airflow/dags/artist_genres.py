import requests
import pandas as pd
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import os

load_dotenv()

def download_top_tags():
    lastfm_api_key = os.getenv('lastfm_api_key')
    artists_df = pd.read_csv('top_artists.csv')

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
        data = response.json()
    
        tags = data['toptags']['tag']
        top_tag = tags[0]['name'] if tags else None

        results.append({'artist': artist, 'genre': top_tag})

    genre_df = pd.DataFrame(results)
    genre_df.to_csv('artist_genres.csv', index=False)

def ingest_top_genres():

    top_genres_local_path = './artist_genres.csv'
    container_name = os.getenv('container_name')
    blob_name = 'artist_genres.csv'
    account_url = os.getenv('Azure_account_url')
    blob_service_client = BlobServiceClient(account_url=account_url, credential=DefaultAzureCredential())
    container_client = blob_service_client.get_container_client(container_name)

    blob_client = container_client.get_blob_client(blob_name)
    with open(top_genres_local_path, 'rb') as data:
        blob_client.upload_blob(data, overwrite=True)


if __name__ == "__main__":
    download_top_tags()
    ingest_top_genres()