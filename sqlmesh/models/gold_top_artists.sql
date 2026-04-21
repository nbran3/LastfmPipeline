MODEL (
    name dbo.gold_top_artists,
    kind FULL
);

WITH artist_country_listeners AS (
    SELECT
        artists.id,
        artists.name,
        artists.listeners as global_listeners,
        artists.playcount as global_playcount,
        geo.country,
        geo.[rank] as country_rank,
        geo.listeners as country_listeners,
        ROUND((CAST(geo.listeners AS FLOAT) / artists.listeners * 100), 2) as percent_of_total_listeners
    FROM dbo.top_artists as artists
    JOIN dbo.geo_top_artists as geo
        ON artists.name = geo.artist
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
    acl.global_listeners,
    acl.global_playcount,
    acl.country as most_popular_country
FROM artist_country_listeners acl
JOIN max_per_artist mpa
    ON acl.name = mpa.name
    AND acl.percent_of_total_listeners = mpa.max_percent