from fastapi import FastAPI, HTTPException
from anthropic import Anthropic
from pydantic import BaseModel
from dotenv import load_dotenv
import os
import pyodbc
import json

load_dotenv()

app = FastAPI(title="Claude Music Generator")
client = Anthropic()

DB_SCHEMA = """
Table: dbo.gold_top_artists
Columns: id, name, genre, global_listeners, global_playcount, most_popular_country, countries_charted_in

Table: dbo.gold_top_tracks
Columns: id, artist, genre, track_name, artist_track_rank, recent_listeners, total_playcount, pct_of_artist_playcount, pct_of_artist_recent_listeners

Table: dbo.gold_top_albums
Columns: id, artist, genre, album_name, playcount, artist_album_rank, percent_of_artist_playcount

Table: dbo.geo_artists
Columns: id, artist, genre, country, country_rank, country_listeners
"""

SQL_SYSTEM = f"""You are a SQL expert for an Azure SQL Database containing Last.fm music data.
Generate a single valid T-SQL SELECT query based on the user's question.
Only use the tables and columns listed in the schema below. Return SQL only, no explanation, no markdown fences.

{DB_SCHEMA}"""

FORMAT_SYSTEM = """You are a music assistant. The user asked a question and you have results from a database query.
Format the results as a helpful natural language response — a list, playlist, or summary depending on what fits best. Keep it concise. Columns like "recent_listeners" 
and "pct_of_artist_recent_listeners" are from the previous two weeks of data. New data is ingested every two weeks on Wednesdays. """


def run_query(sql: str) -> list[dict]:
    connection_string = os.getenv("AZURE_SQL_CONNECTIONSTRING")
    with pyodbc.connect(connection_string) as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            columns = [col[0] for col in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]


class QueryRequest(BaseModel):
    user_input: str


@app.post("/query")
def query(request: QueryRequest):
    try:
        # Step 1: generate SQL
        sql_response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=1000,
            system=SQL_SYSTEM,
            messages=[{"role": "user", "content": request.user_input}],
        )
        sql = sql_response.content[0].text.strip()

        # Step 2: execute against Azure SQL
        results = run_query(sql)

        # Step 3: format as natural language
        format_response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=1000,
            system=FORMAT_SYSTEM,
            messages=[{
                "role": "user",
                "content": f"Question: {request.user_input}\n\nResults: {json.dumps(results, default=str)}",
            }],
        )

        return {
            "answer": format_response.content[0].text,
            "sql": sql,
            "results": results,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))