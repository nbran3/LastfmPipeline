MODEL (
    name dbo.gold_top_albums,
    kind FULL
);

SELECT albums.id,
        albums.artist,
        genres.genre, 
        albums.album_name, 
        albums.playcount,
        albums.top_album as artist_album_rank, 
        ROUND((CAST(albums.playcount AS FLOAT) / SUM(albums.playcount) OVER (PARTITION BY albums.artist) * 100), 2) as percent_of_artist_playcount
FROM dbo.artist_albums as albums
JOIN dbo.artist_genres as genres
    ON albums.artist = genres.artist