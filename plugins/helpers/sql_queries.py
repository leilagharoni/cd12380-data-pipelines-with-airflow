class SqlQueries:
    songplay_table_insert = ("""
        SELECT DISTINCT
            md5(events.sessionid || events.start_time) AS songplay_id,
            events.start_time,
            events.userid,
            events.level,
            songs.song_id,
            songs.artist_id,
            events.sessionid,
            events.location,
            events.useragent
        FROM (
            SELECT
                TIMESTAMP 'epoch' + ts/1000 * interval '1 second'
                    AS start_time,
                *
            FROM staging_events
            WHERE page = 'NextSong'
              AND userid IS NOT NULL
        ) events
        LEFT JOIN staging_songs songs
            ON events.song = songs.title
            AND events.artist = songs.artist_name
            AND events.length = songs.duration
        WHERE NOT EXISTS (
            SELECT 1
            FROM songplays existing
            WHERE existing.playid =
                md5(events.sessionid || events.start_time)
        )
    """)

    user_table_insert = ("""
        SELECT userid, firstname, lastname, gender, level
        FROM (
            SELECT
                userid,
                firstname,
                lastname,
                gender,
                level,
                ROW_NUMBER() OVER (
                    PARTITION BY userid
                    ORDER BY ts DESC
                ) AS row_num
            FROM staging_events
            WHERE page = 'NextSong'
              AND userid IS NOT NULL
        ) latest_users
        WHERE row_num = 1
    """)

    song_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        SELECT
            artist_id,
            artist_name,
            artist_location,
            artist_latitude,
            artist_longitude
        FROM (
            SELECT
                artist_id,
                artist_name,
                artist_location,
                artist_latitude,
                artist_longitude,
                ROW_NUMBER() OVER (
                    PARTITION BY artist_id
                    ORDER BY song_id, artist_name, artist_location,
                             artist_latitude, artist_longitude
                ) AS row_num
            FROM staging_songs
            WHERE artist_id IS NOT NULL
        ) ranked_artists
        WHERE row_num = 1
    """)

    time_table_insert = ("""
        SELECT DISTINCT
            start_time,
            extract(hour from start_time),
            extract(day from start_time),
            extract(week from start_time),
            extract(month from start_time),
            extract(year from start_time),
            extract(dayofweek from start_time)
        FROM songplays
    """)