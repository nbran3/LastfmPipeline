import io
import pandas as pd
from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient
import json
from airflow import DAG
from airflow.sdk import Variable
from airflow.providers.standard.operators.python import PythonOperator
from mssql_python import connect
import os


default_args = {
    'start_date': '2026-01-01',
    'retries': 3,
    'retry_delay': 300
}

def ingest_to_azure_sql_top_artists():
    creds = json.loads(Variable.get('properties'))
    credential = ClientSecretCredential(
        tenant_id=creds['tenantId'],
        client_id=creds['clientId'],
        client_secret=creds['client_secret']
    )

    print("Connecting to Azure Blob Storage... for top artists.csv")

    blob_service_client = BlobServiceClient(account_url=Variable.get('azure_account'), credential=credential)
    blob_client = blob_service_client.get_blob_client(container=Variable.get('container_name'), blob='top_artists.csv')

    artists_df = blob_client.download_blob().readall()
    artists_df = pd.read_csv(io.BytesIO(artists_df))

    artists_df['listeners'] = pd.to_numeric(artists_df['listeners'], errors='coerce').fillna(0).astype(int)
    artists_df['playcount'] = pd.to_numeric(artists_df['playcount'], errors='coerce').fillna(0).astype(int)
    artists_df = artists_df.where(pd.notnull(artists_df), None)

    connection_string = os.getenv('AZURE_SQL_CONNECTIONSTRING')
    conn = connect(connection_string)

    
    print("Creating top_artists table if it doesn't exist...")
    
    cursor = conn.cursor()
    print("Inserting data into top_artists table...")
    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'top_artists')
        CREATE TABLE top_artists (
            id INT IDENTITY(1,1) PRIMARY KEY,
            name NVARCHAR(255),
            playcount BIGINT,
            listeners BIGINT
        )
    """)
    conn.commit()

    for _, row in artists_df.iterrows():
        cursor.execute("INSERT INTO top_artists (name, playcount, listeners) VALUES (?, ?, ?)", row['name'], row['playcount'], row['listeners'])
    conn.commit()

    print ("Data ingestion for top_artists completed successfully.")
    cursor.close()
    conn.close()
    

# def ingest_to_azure_sql_top_tracks():
#     creds = json.loads(Variable.get('properties'))
#     credential = ClientSecretCredential(
#         tenant_id=creds['tenantId'],
#         client_id=creds['clientId'],
#         client_secret=creds['client_secret']
#     )

#     print("Connecting to Azure Blob Storage to download top_tracks.csv...")
#     blob_service_client = BlobServiceClient(account_url=Variable.get('azure_account'), credential=credential)
#     blob_client = blob_service_client.get_blob_client(container=Variable.get('container_name'), blob='top_tracks.csv')

#     tracks_df = blob_client.download_blob().readall()
#     tracks_df = pd.read_csv(io.BytesIO(tracks_df))
#     tracks_df['listeners'] = pd.to_numeric(tracks_df['listeners'], errors='coerce').fillna(0).astype(int)
#     tracks_df['playcount'] = pd.to_numeric(tracks_df['playcount'], errors='coerce').fillna(0).astype(int)

#     conn = pyodbc.connect(
#         f"Driver={{ODBC Driver 18 for SQL Server}};"
#         f"Server={Variable.get('sql_server')};"
#         f"Database={Variable.get('sql_database')};"
#         f"UID={Variable.get('sql_user')};"
#         f"PWD={Variable.get('sql_password')};")
    
#     cursor = conn.cursor()
#     cursor.execute("""
#         IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'top_tracks')
#         CREATE TABLE top_tracks (
#             id INT IDENTITY(1,1) PRIMARY KEY,
#             name NVARCHAR(255),
#             playcount BIGINT,
#             listeners BIGINT
#         )
#     """)
#     conn.commit()

#     for _, row in tracks_df.iterrows():
#         cursor.execute("INSERT INTO top_tracks (name, playcount, listeners) VALUES (?, ?, ?)", row['name'], row['playcount'], row['listeners'])
#     conn.commit()
#     print("Data ingestion for top_tracks completed successfully.")
#     cursor.close()
#     conn.close()


def ingest_to_azure_sql_artists_genres():
    creds = json.loads(Variable.get('properties'))
    credential = ClientSecretCredential(
        tenant_id=creds['tenantId'],
        client_id=creds['clientId'],
        client_secret=creds['client_secret']
    )

    print("Connecting to Azure Blob Storage... for artist genres.csv")
    blob_service_client = BlobServiceClient(account_url=Variable.get('azure_account'), credential=credential)
    blob_client = blob_service_client.get_blob_client(container=Variable.get('container_name'), blob='artist_genres.csv')

    artists_genres_df = blob_client.download_blob().readall()
    artists_genres_df = pd.read_csv(io.BytesIO(artists_genres_df))
    artists_genres_df = artists_genres_df.where(pd.notnull(artists_genres_df), None)
    
    connection_string = os.getenv('AZURE_SQL_CONNECTIONSTRING')
    conn = connect(connection_string)
    
    cursor = conn.cursor()
    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'artist_genres')
        CREATE TABLE artist_genres (
            id INT IDENTITY(1,1) PRIMARY KEY,
            artist NVARCHAR(255),
            genre NVARCHAR(255) NULL
        )
    """)
    conn.commit()

    for _, row in artists_genres_df.iterrows():
        cursor.execute("INSERT INTO artist_genres (artist, genre) VALUES (?, ?)", row['artist'], row['genre'])
    conn.commit()
    print("Data ingestion for artist_genres completed successfully.")
    cursor.close()
    conn.close()

def ingest_to_azure_sql_artist_tracks():
    creds = json.loads(Variable.get('properties'))
    credential = ClientSecretCredential(
        tenant_id=creds['tenantId'],
        client_id=creds['clientId'],
        client_secret=creds['client_secret']
    )

    print("Connecting to Azure Blob Storage... for artist_tracks.csv")
    blob_service_client = BlobServiceClient(account_url=Variable.get('azure_account'), credential=credential)
    blob_client = blob_service_client.get_blob_client(container=Variable.get('container_name'), blob='artist_tracks.csv')

    artists_tracks_df = blob_client.download_blob().readall()
    artists_tracks_df = pd.read_csv(io.BytesIO(artists_tracks_df))
    artists_tracks_df = artists_tracks_df.where(pd.notnull(artists_tracks_df), None)

    connection_string = os.getenv('AZURE_SQL_CONNECTIONSTRING')
    conn = connect(connection_string)
    
    cursor = conn.cursor()
    cursor.execute("""
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'artist_tracks')
    CREATE TABLE artist_tracks (
        id INT IDENTITY(1,1) PRIMARY KEY,
        artist NVARCHAR(255),
        name NVARCHAR(255),
        track_ranking INT NULL,
        listeners BIGINT NULL,
        playcount BIGINT NULL
    )
""")
    conn.commit()

    for _, row in artists_tracks_df.iterrows():
        cursor.execute(
        "INSERT INTO artist_tracks (artist, name, track_ranking, listeners, playcount) VALUES (?, ?, ?, ?, ?)",
        row['artist'], row['track_name'], row['track_ranking'], row['listeners'], row['playcount']
    )   
    conn.commit()


def ingest_to_azure_sql_artist_albums():
    creds = json.loads(Variable.get('properties'))
    credential = ClientSecretCredential(
        tenant_id=creds['tenantId'],
        client_id=creds['clientId'],
        client_secret=creds['client_secret']
    )

    print("Connecting to Azure Blob Storage... for artist albums.csv")
    blob_service_client = BlobServiceClient(account_url=Variable.get('azure_account'), credential=credential)
    blob_client = blob_service_client.get_blob_client(container=Variable.get('container_name'), blob='artist_albums.csv')

    artists_albums_df = blob_client.download_blob().readall()
    artists_albums_df = pd.read_csv(io.BytesIO(artists_albums_df))
    artists_albums_df = artists_albums_df.where(pd.notnull(artists_albums_df), None)
    
    connection_string = os.getenv('AZURE_SQL_CONNECTIONSTRING')
    conn = connect(connection_string)
    
    cursor = conn.cursor()
    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'artist_albums')
        CREATE TABLE artist_albums (
            id INT IDENTITY(1,1) PRIMARY KEY,
            artist NVARCHAR(255),
            album_name NVARCHAR(255) NULL,
            top_album INT NULL,
            playcount BIGINT NULL
        )
    """)
    conn.commit()

    for _, row in artists_albums_df.iterrows():
        cursor.execute("INSERT INTO artist_albums (artist, album_name, top_album, playcount) VALUES (?, ?, ?, ?)", row['artist'], row['album_name'], row['top_album'], row['playcount'])
    conn.commit()
    print("Data ingestion for artist_albums completed successfully.")
    cursor.close()
    conn.close()

def ingest_to_azure_sql_geo_artists():
    creds = json.loads(Variable.get('properties'))
    credential = ClientSecretCredential(
        tenant_id=creds['tenantId'],
        client_id=creds['clientId'],
        client_secret=creds['client_secret']
    )

    print("Connecting to Azure Blob Storage... for geo artists.csv")
    blob_service_client = BlobServiceClient(account_url=Variable.get('azure_account'), credential=credential)
    blob_client = blob_service_client.get_blob_client(container=Variable.get('container_name'), blob='geo_top_artists.csv')

    artists_geo_df = blob_client.download_blob().readall()
    artists_geo_df = pd.read_csv(io.BytesIO(artists_geo_df))
    artists_geo_df = artists_geo_df.where(pd.notnull(artists_geo_df), None)
    
    connection_string = os.getenv('AZURE_SQL_CONNECTIONSTRING')
    conn = connect(connection_string)
    
    cursor = conn.cursor()
    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'geo_top_artists')
        CREATE TABLE geo_top_artists (
            id INT IDENTITY(1,1) PRIMARY KEY,
            country NVARCHAR(100),
            rank int NULL,
            artist nvarchar(255) NULL,
            listeners BIGINT NULL
        )
    """)
    conn.commit()

    for _, row in artists_geo_df.iterrows():
        cursor.execute("INSERT INTO geo_top_artists (country, rank, artist, listeners) VALUES (?, ?, ?, ?)", row['country'], row['rank'], row['artist'], row['listeners'])
    conn.commit()
    print("Data ingestion for geo_top_artists completed successfully.")
    cursor.close()
    conn.close()



with DAG('ingest_to_sql_dag', default_args=default_args, catchup=False) as dag:
    ingest_top_artists_task = PythonOperator(
        task_id='ingest_top_artists',
        python_callable=ingest_to_azure_sql_top_artists
    )

    ingest_artist_genres_task = PythonOperator(
        task_id='ingest_artist_genres',
        python_callable=ingest_to_azure_sql_artists_genres
    )

    ingest_artist_tracks_task = PythonOperator(
        task_id='ingest_artist_tracks',
        python_callable=ingest_to_azure_sql_artist_tracks
    )
    ingest_artist_albums_task = PythonOperator(
        task_id='ingest_artist_albums',
        python_callable=ingest_to_azure_sql_artist_albums
    )

    ingest_geo_top_artist_task = PythonOperator(
        task_id = 'ingest_geo_top_artist_task',
        python_callable=ingest_to_azure_sql_geo_artists

    )

    ingest_top_artists_task >> ingest_artist_genres_task >> ingest_artist_tracks_task >> ingest_artist_albums_task >> ingest_geo_top_artist_task