MODEL (
    name dbo.geo_artists,    
    kind full)
;


SELECT geo.id, 
    geo.artist, 
    genres.genre, 
    geo.country, 
    geo.[rank] as country_rank, 
    geo.listeners as country_listeners
FROM dbo.geo_top_artists as geo
JOIN dbo.artist_genres as genres
ON geo.artist = genres.artist