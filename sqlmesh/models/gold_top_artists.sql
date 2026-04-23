MODEL (
    name dbo.gold_top_artists,
    kind FULL
);

WITH artist_country_listeners AS (
    SELECT
        artists.id,
        artists.name,
        artists.listeners as Global_Listeners,
        artists.playcount as Global_Playcount,
        genres.genre as Genre,
        geo.country,
        geo.[rank] as Country_Rank,
        geo.listeners as Country_Listeners,
        ROUND((CAST(geo.listeners AS FLOAT) / artists.listeners * 100), 2) as percent_of_total_listeners,
        COUNT(geo.country) OVER (PARTITION BY artists.name) as countries_charted_in
    FROM dbo.top_artists as artists
   JOIN dbo.geo_top_artists as geo
        ON artists.name = geo.artist
    JOIN dbo.artist_genres as genres
        ON artists.name = genres.artist
),
max_per_artist AS (
    SELECT
        name,
        MAX(percent_of_total_listeners) as max_percent
    FROM artist_country_listeners
    GROUP BY name
)
SELECT
    acl.id,
    acl.name,
    acl.genre,
    acl.global_listeners,
    acl.global_playcount,
    acl.country as most_popular_country,
    acl.countries_charted_in
FROM artist_country_listeners acl
JOIN max_per_artist mpa
    ON acl.name = mpa.name
    AND acl.percent_of_total_listeners = mpa.max_percent