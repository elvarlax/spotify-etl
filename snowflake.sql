---------------------DATABASE------------------------

CREATE OR REPLACE DATABASE spotify;

-----------------------SCHEMAS------------------------

CREATE OR REPLACE SCHEMA spotify.staging;
CREATE OR REPLACE SCHEMA spotify.analytics;
CREATE OR REPLACE SCHEMA spotify.pipes;
CREATE OR REPLACE SCHEMA spotify.streams;
CREATE OR REPLACE SCHEMA spotify.tasks;

-----------------------FILE FORMAT------------------------

CREATE OR REPLACE FILE FORMAT spotify.staging.my_csv_format
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY='"';

-------------------STORAGE INTEGRATION--------------------

CREATE OR REPLACE STORAGE INTEGRATION azure_spotify_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'AZURE'
  ENABLED = TRUE
  AZURE_TENANT_ID = 'AZURE_TENANT_ID'
  STORAGE_ALLOWED_LOCATIONS = ('azure://STORAGE_ACCOUNT_NAME.blob.core.windows.net/STORAGE_CONTAINER/transformed_data/');
  
---------------------------STAGE--------------------------

CREATE OR REPLACE STAGE spotify.staging.azure_spotify_stage
  URL = 'azure://STORAGE_ACCOUNT_NAME.blob.core.windows.net/STORAGE_CONTAINER/transformed_data/'
  STORAGE_INTEGRATION = azure_spotify_integration
  FILE_FORMAT = spotify.staging.my_csv_format;

-------------------------STAGING--------------------------

CREATE OR REPLACE TABLE staging.albums (
    album_id VARCHAR(22) PRIMARY KEY,
    name VARCHAR(100),
    release_date DATE,
    total_tracks INTEGER,
    url VARCHAR(100)
);

CREATE OR REPLACE TABLE staging.artists (
    artist_id VARCHAR(22) PRIMARY KEY,
    artist_name VARCHAR(100),
    external_url VARCHAR(100)
);

CREATE OR REPLACE TABLE staging.songs (
    song_id VARCHAR(22) PRIMARY KEY,
    song_name VARCHAR(100),
    duration_ms BIGINT,
    url VARCHAR(100),
    popularity INTEGER,
    song_added TIMESTAMP,
    album_id VARCHAR(22),
    artist_id VARCHAR(22),
    FOREIGN KEY (album_id) REFERENCES staging.albums(album_id),
    FOREIGN KEY (artist_id) REFERENCES staging.artists(artist_id)
);

-----------------------DIMENSIONS & FACTS----------------------

CREATE OR REPLACE TABLE spotify.analytics.dim_albums (
    album_id VARCHAR(22) NOT NULL,
    name VARCHAR(100),
    release_date DATE,
    total_tracks NUMBER(38,0),
    url VARCHAR(100),
    PRIMARY KEY (album_id)
);

CREATE OR REPLACE TABLE spotify.analytics.dim_artists (
    artist_id VARCHAR(22) NOT NULL,
    artist_name VARCHAR(100),
    external_url VARCHAR(100),
    PRIMARY KEY (artist_id)
);

CREATE OR REPLACE TABLE spotify.analytics.dim_songs (
    song_id VARCHAR(22) NOT NULL,
    song_name VARCHAR(100),
    duration_ms NUMBER(38,0),
    url VARCHAR(100),
    PRIMARY KEY (song_id)
);

CREATE OR REPLACE TABLE spotify.analytics.fact_songs (
    song_id VARCHAR(22) NOT NULL,
    album_id VARCHAR(22) NOT NULL,
    artist_id VARCHAR(22) NOT NULL,
    popularity NUMBER(38,0),
    duration_ms NUMBER(38,0),
    song_added TIMESTAMP,
    PRIMARY KEY (song_id),
    FOREIGN KEY (album_id) REFERENCES analytics.dim_albums(album_id),
    FOREIGN KEY (artist_id) REFERENCES analytics.dim_artists(artist_id)
);

-------------------NOTIFICATION INTEGRATIONS-------------------

CREATE OR REPLACE NOTIFICATION INTEGRATION albums_notification_integration
  ENABLED = TRUE
  TYPE = QUEUE
  NOTIFICATION_PROVIDER = AZURE_STORAGE_QUEUE
  AZURE_STORAGE_QUEUE_PRIMARY_URI = 'https://STORAGE_ACCOUNT_NAME.queue.core.windows.net/AZURE_STORAGE_QUEUE_NAME_ALBUMS'
  AZURE_TENANT_ID = 'AZURE_TENANT_ID';

CREATE OR REPLACE NOTIFICATION INTEGRATION artists_notification_integration
  ENABLED = TRUE
  TYPE = QUEUE
  NOTIFICATION_PROVIDER = AZURE_STORAGE_QUEUE
  AZURE_STORAGE_QUEUE_PRIMARY_URI = 'https://STORAGE_ACCOUNT_NAME.queue.core.windows.net/AZURE_STORAGE_QUEUE_NAME_ARTISTS'
  AZURE_TENANT_ID = 'AZURE_TENANT_ID';

CREATE OR REPLACE NOTIFICATION INTEGRATION songs_notification_integration
  ENABLED = TRUE
  TYPE = QUEUE
  NOTIFICATION_PROVIDER = AZURE_STORAGE_QUEUE
  AZURE_STORAGE_QUEUE_PRIMARY_URI = 'https://STORAGE_ACCOUNT_NAME.queue.core.windows.net/AZURE_STORAGE_QUEUE_NAME_SONGS'
  AZURE_TENANT_ID = 'AZURE_TENANT_ID';

-----------------------------PIPES-----------------------------

CREATE OR REPLACE PIPE spotify.pipes.albums_pipe
AUTO_INGEST = TRUE
INTEGRATION = albums_notification_integration
AS
COPY INTO spotify.staging.albums (album_id, name, release_date, total_tracks, url)
FROM @spotify.staging.azure_spotify_stage/album_data/album_transformed_
PATTERN = '.*[0-9]{4}-[0-9]{2}-[0-9]{2}_[0-9]{2}-[0-9]{2}-[0-9]{2}\.csv'
FILE_FORMAT = (FORMAT_NAME = 'spotify.staging.my_csv_format');

CREATE OR REPLACE PIPE spotify.pipes.artists_pipe
AUTO_INGEST = TRUE
INTEGRATION = artists_notification_integration
AS
COPY INTO spotify.staging.artists (artist_id, artist_name, external_url)
FROM @spotify.staging.azure_spotify_stage/artist_data/artist_transformed_
PATTERN = '.*[0-9]{4}-[0-9]{2}-[0-9]{2}_[0-9]{2}-[0-9]{2}-[0-9]{2}\.csv'
FILE_FORMAT = (FORMAT_NAME = 'spotify.staging.my_csv_format');

CREATE OR REPLACE PIPE spotify.pipes.songs_pipe
AUTO_INGEST = TRUE
INTEGRATION = songs_notification_integration
AS
COPY INTO spotify.staging.songs (song_id, song_name, duration_ms, url, popularity, song_added, album_id, artist_id)
FROM @spotify.staging.azure_spotify_stage/songs_data/song_transformed_
PATTERN = '.*[0-9]{4}-[0-9]{2}-[0-9]{2}_[0-9]{2}-[0-9]{2}-[0-9]{2}\.csv'
FILE_FORMAT = (FORMAT_NAME = 'spotify.staging.my_csv_format');

-----------------------------STREAMS-----------------------------

CREATE OR REPLACE STREAM spotify.streams.albums_stream ON TABLE spotify.staging.albums;
CREATE OR REPLACE STREAM spotify.streams.artists_stream ON TABLE spotify.staging.artists;
CREATE OR REPLACE STREAM spotify.streams.songs_stream ON TABLE spotify.staging.songs;

-----------------------------TASKS-----------------------------

CREATE OR REPLACE TASK spotify.tasks.dim_albums_task
WAREHOUSE = COMPUTE_WH
SCHEDULE = '60 MINUTE'
AS
BEGIN
MERGE INTO spotify.analytics.dim_albums AS target
USING (
    WITH staging_albums AS (
        SELECT album_id, name, release_date, total_tracks, url,
               ROW_NUMBER() OVER (PARTITION BY album_id ORDER BY album_id) AS row_num
        FROM staging.albums
    )
    SELECT album_id, name, release_date, total_tracks, url
    FROM staging_albums
    WHERE row_num = 1
) AS source
ON target.album_id = source.album_id
WHEN MATCHED THEN
    UPDATE SET name = source.name,
               release_date = source.release_date,
               total_tracks = source.total_tracks,
               url = source.url
WHEN NOT MATCHED THEN
    INSERT (album_id, name, release_date, total_tracks, url)
    VALUES (source.album_id, source.name, source.release_date, source.total_tracks, source.url);
END;

CREATE OR REPLACE TASK spotify.tasks.dim_artists_task
WAREHOUSE = COMPUTE_WH
AFTER spotify.tasks.dim_albums_task
AS
BEGIN
MERGE INTO spotify.analytics.dim_artists AS target
USING (
    WITH staging_artists AS (
        SELECT artist_id, artist_name, external_url,
               ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY artist_id) AS row_num
        FROM staging.artists
    )
    SELECT artist_id, artist_name, external_url
    FROM staging_artists
    WHERE row_num = 1
) AS source
ON target.artist_id = source.artist_id
WHEN MATCHED THEN
    UPDATE SET artist_name = source.artist_name,
               external_url = source.external_url
WHEN NOT MATCHED THEN
    INSERT (artist_id, artist_name, external_url)
    VALUES (source.artist_id, source.artist_name, source.external_url);
END;

CREATE OR REPLACE TASK spotify.tasks.dim_songs_task
WAREHOUSE = COMPUTE_WH
AFTER spotify.tasks.dim_artists_task
AS
BEGIN
  MERGE INTO spotify.analytics.dim_songs AS target
  USING (
    WITH staging_songs AS (
        SELECT song_id, song_name, duration_ms, url,
               ROW_NUMBER() OVER (PARTITION BY song_id ORDER BY song_id) AS row_num
        FROM staging.songs
    )
    SELECT song_id, song_name, duration_ms, url
    FROM staging_songs
    WHERE row_num = 1
  ) AS source
  ON target.song_id = source.song_id
  WHEN MATCHED THEN
    UPDATE SET song_name = source.song_name,
               duration_ms = source.duration_ms,
               url = source.url
  WHEN NOT MATCHED THEN
    INSERT (song_id, song_name, duration_ms, url)
    VALUES (source.song_id, source.song_name, source.duration_ms, source.url);
END;

CREATE OR REPLACE TASK spotify.tasks.fact_songs_task
WAREHOUSE = COMPUTE_WH
AFTER spotify.tasks.dim_songs_task
AS
BEGIN
  MERGE INTO spotify.analytics.fact_songs AS target
  USING (
    WITH staging_songs AS (
        SELECT song_id, album_id, artist_id, popularity, duration_ms, song_added,
               ROW_NUMBER() OVER (PARTITION BY song_id, album_id, artist_id ORDER BY song_id, album_id, artist_id) AS row_num
        FROM staging.songs
    )
    SELECT song_id, album_id, artist_id, popularity, duration_ms, song_added
    FROM staging_songs
    WHERE row_num = 1
  ) AS source
  ON target.song_id = source.song_id
  AND target.album_id = source.album_id
  AND target.artist_id = source.artist_id
  WHEN NOT MATCHED THEN 
    INSERT (song_id, album_id, artist_id, popularity, duration_ms, song_added)
    VALUES (source.song_id, source.album_id, source.artist_id, source.popularity, source.duration_ms, source.song_added);
END;

----------------------RESUME/SUSPEND TASKS------------------------
-- RESUME TASKS
ALTER TASK spotify.tasks.fact_songs_task RESUME;
ALTER TASK spotify.tasks.dim_songs_task RESUME;
ALTER TASK spotify.tasks.dim_artists_task RESUME;
ALTER TASK spotify.tasks.dim_albums_task RESUME;

-- SUSPEND TASKS
ALTER TASK spotify.tasks.dim_albums_task SUSPEND;
ALTER TASK spotify.tasks.dim_artists_task SUSPEND;
ALTER TASK spotify.tasks.dim_songs_task SUSPEND;
ALTER TASK spotify.tasks.fact_songs_task SUSPEND;

----------------------------ANALYTICS-----------------------------
-- Top 10 Songs by Popularity
SELECT s.song_name, f.popularity
FROM spotify.analytics.fact_songs f
INNER JOIN spotify.analytics.dim_songs s ON f.song_id = s.song_id
ORDER BY popularity DESC
LIMIT 10;

-- Average Song Duration
SELECT AVG(duration_ms) AS average_duration_ms
FROM spotify.analytics.fact_songs;

-- Total Number of Songs by Artist Name
SELECT a.artist_name, COUNT(f.song_id) AS total_songs
FROM spotify.analytics.dim_artists a
LEFT JOIN spotify.analytics.fact_songs f ON f.artist_id = a.artist_id
GROUP BY a.artist_name
ORDER BY total_songs DESC;

-- Album Popularity Distribution
SELECT a.name, f.popularity
FROM spotify.analytics.fact_songs f
INNER JOIN spotify.analytics.dim_albums a ON f.album_id = a.album_id
ORDER BY f.popularity DESC;

-- Most Popular Artists
SELECT a.artist_name, SUM(popularity) AS total_popularity
FROM spotify.analytics.fact_songs f
INNER JOIN spotify.analytics.dim_artists a ON f.artist_id = a.artist_id
GROUP BY a.artist_name
ORDER BY total_popularity DESC;

-- Average Popularity by Album Release Year:
SELECT YEAR(a.release_date) AS release_year, AVG(f.popularity) AS average_popularity
FROM spotify.analytics.fact_songs f
INNER JOIN spotify.analytics.dim_albums a ON f.album_id = a.album_id
GROUP BY release_year
ORDER BY release_year;

-- Top Artists by Total Duration of Songs
SELECT a.artist_name, SUM(s.duration_ms) AS total_duration_ms
FROM spotify.analytics.fact_songs f
INNER JOIN spotify.analytics.dim_artists a ON f.artist_id = a.artist_id
INNER JOIN spotify.analytics.dim_songs s ON f.song_id = s.song_id
GROUP BY a.artist_name
ORDER BY total_duration_ms DESC;

-- Songs Added Over Time
SELECT DATE_TRUNC('month', song_added) AS month_added, COUNT(song_id) AS songs_added
FROM spotify.analytics.fact_songs
GROUP BY month_added
ORDER BY month_added;

-- Album with the Longest Duration
SELECT a.name AS album_name, SUM(s.duration_ms) AS total_duration_ms
FROM spotify.analytics.fact_songs f
INNER JOIN spotify.analytics.dim_albums a ON f.album_id = a.album_id
INNER JOIN spotify.analytics.dim_songs s ON f.song_id = s.song_id
GROUP BY album_name
ORDER BY total_duration_ms DESC
LIMIT 1;