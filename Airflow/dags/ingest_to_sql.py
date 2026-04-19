import io
import pandas as pd
from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient
import json
from airflow import DAG
from airflow.sdk import Variable
from airflow.providers.standard.operators.python import PythonOperator
import pyodbc

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

    print("Connecting to Azure SQL Database...")
    
    conn = pyodbc.connect(
        f"Driver={{ODBC Driver 18 for SQL Server}};"
        f"Server={Variable.get('sql_server')};"
        f"Database={Variable.get('sql_database')};"
        f"UID={Variable.get('sql_user')};"
        f"PWD={Variable.get('sql_password')};")
    
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
    

def ingest_to_azure_sql_top_tracks():
    creds = json.loads(Variable.get('properties'))
    credential = ClientSecretCredential(
        tenant_id=creds['tenantId'],
        client_id=creds['clientId'],
        client_secret=creds['client_secret']
    )

    print("Connecting to Azure Blob Storage to download top_tracks.csv...")
    blob_service_client = BlobServiceClient(account_url=Variable.get('azure_account'), credential=credential)
    blob_client = blob_service_client.get_blob_client(container=Variable.get('container_name'), blob='top_tracks.csv')

    tracks_df = blob_client.download_blob().readall()
    tracks_df = pd.read_csv(io.BytesIO(tracks_df))
    tracks_df['listeners'] = pd.to_numeric(tracks_df['listeners'], errors='coerce').fillna(0).astype(int)
    tracks_df['playcount'] = pd.to_numeric(tracks_df['playcount'], errors='coerce').fillna(0).astype(int)

    conn = pyodbc.connect(
        f"Driver={{ODBC Driver 18 for SQL Server}};"
        f"Server={Variable.get('sql_server')};"
        f"Database={Variable.get('sql_database')};"
        f"UID={Variable.get('sql_user')};"
        f"PWD={Variable.get('sql_password')};")
    
    cursor = conn.cursor()
    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'top_tracks')
        CREATE TABLE top_tracks (
            id INT IDENTITY(1,1) PRIMARY KEY,
            name NVARCHAR(255),
            playcount BIGINT,
            listeners BIGINT
        )
    """)
    conn.commit()

    for _, row in tracks_df.iterrows():
        cursor.execute("INSERT INTO top_tracks (name, playcount, listeners) VALUES (?, ?, ?)", row['name'], row['playcount'], row['listeners'])
    conn.commit()
    print("Data ingestion for top_tracks completed successfully.")
    cursor.close()
    conn.close()


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
    
    conn = pyodbc.connect(
        f"Driver={{ODBC Driver 18 for SQL Server}};"
        f"Server={Variable.get('sql_server')};"
        f"Database={Variable.get('sql_database')};"
        f"UID={Variable.get('sql_user')};"
        f"PWD={Variable.get('sql_password')};")
    
    cursor = conn.cursor()
    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'artist_genres')
        CREATE TABLE artist_genres (
            id INT IDENTITY(1,1) PRIMARY KEY,
            artist NVARCHAR(255),
            genre NVARCHAR(255)
        )
    """)
    conn.commit()

    for _, row in artists_genres_df.iterrows():
        cursor.execute("INSERT INTO artist_genres (artist, genre) VALUES (?, ?)", row['artist'], row['genre'])
    conn.commit()
    print("Data ingestion for artist_genres completed successfully.")
    cursor.close()
    conn.close()


with DAG('ingest_to_sql_dag', default_args=default_args, catchup=False) as dag:
    ingest_top_artists_task = PythonOperator(
        task_id='ingest_top_artists',
        python_callable=ingest_to_azure_sql_top_artists
    )

    ingest_top_tracks_task = PythonOperator(
        task_id='ingest_top_tracks',
        python_callable=ingest_to_azure_sql_top_tracks
    )

    ingest_artist_genres_task = PythonOperator(
        task_id='ingest_artist_genres',
        python_callable=ingest_to_azure_sql_artists_genres
    )

    ingest_top_artists_task >> ingest_top_tracks_task >> ingest_artist_genres_task