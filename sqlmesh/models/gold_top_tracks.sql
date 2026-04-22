MODEL (
    name dbo.gold_top_tracks,
    kind FULL
);

SELECT 
    tracks.id, 
    tracks.artist,
    genres.genre,
    tracks.name as track_name,
    tracks.track_ranking as artist_track_rank,
    tracks.listeners as recent_listeners,
    tracks.playcount as total_playcount,
    ROUND((CAST(tracks.playcount AS FLOAT) / artists.playcount * 100), 2) as pct_of_artist_playcount,
    ROUND((CAST(tracks.listeners AS FLOAT) / artists.listeners * 100), 2) as pct_of_artist_recent_listeners
FROM dbo.artist_tracks as tracks
JOIN dbo.artist_genres as genres
    ON tracks.artist = genres.artist
JOIN dbo.top_artists as artists
    ON tracks.artist = artists.name
