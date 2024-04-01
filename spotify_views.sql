-- Drop the existing database if it exists
IF EXISTS (SELECT * FROM sys.databases WHERE name = 'spotify_dw')
BEGIN
    USE master
    DROP DATABASE spotify_dw;
END

-- Create a new database and use it
CREATE DATABASE spotify_dw;
USE spotify_dw;

DROP VIEW IF EXISTS dbo.v_albums
GO
DROP VIEW IF EXISTS dbo.v_artists
GO
DROP VIEW IF EXISTS dbo.v_songs
GO

CREATE OR ALTER PROCEDURE dbo.CreateSpotifyViews
    @storage_account NVARCHAR(100),
    @storage_container NVARCHAR(100)
AS
BEGIN
    -- Construct the dynamic SQL to create views for albums, artists, and songs
    
    -- Define the base path for data
    DECLARE @base_path NVARCHAR(200);
    SET @base_path = CONCAT('abfss://', @storage_account, '@', @storage_container, '.dfs.core.windows.net/transformed_data/');

    -- Spotify Albums
    DECLARE @sql_albums NVARCHAR(MAX);
    SET @sql_albums = N'
    CREATE VIEW dbo.v_albums
    AS
    SELECT DISTINCT
        album_id,
        name,
        release_date,
        total_tracks,
        url
    FROM OPENROWSET(
        BULK ''' + @base_path + 'album_data/*.csv'',
        FORMAT = ''CSV'',
        PARSER_VERSION = ''2.0'',
        FIRSTROW = 2
    ) WITH (
        album_id NVARCHAR(22),
        name NVARCHAR(100),
        release_date DATE,
        total_tracks INT,
        url NVARCHAR(100)
    ) AS rows;
    ';
    
    -- Spotify Artists
    DECLARE @sql_artists NVARCHAR(MAX);
    SET @sql_artists = N'
    CREATE VIEW dbo.v_artists
    AS
    SELECT DISTINCT
        artist_id,
        artist_name,
        external_url
    FROM OPENROWSET(
        BULK ''' + @base_path + 'artist_data/*.csv'',
        FORMAT = ''CSV'',
        PARSER_VERSION = ''2.0'',
        FIRSTROW = 2
    ) WITH (
        artist_id NVARCHAR(22),
        artist_name NVARCHAR(100),
        external_url NVARCHAR(100)
    ) AS rows;
    ';
    
    -- Spotify Songs
    DECLARE @sql_songs NVARCHAR(MAX);
    SET @sql_songs = N'
    CREATE VIEW dbo.v_songs
    AS
    SELECT DISTINCT
        song_id,
        song_name,
        duration_ms,
        url,
        popularity,
        song_added,
        album_id,
        artist_id
    FROM OPENROWSET(
        BULK ''' + @base_path + 'songs_data/*.csv'',
        FORMAT = ''CSV'',
        PARSER_VERSION = ''2.0'',
        FIRSTROW = 2
    ) WITH (
        song_id NVARCHAR(22),
        song_name NVARCHAR(100),
        duration_ms BIGINT,
        url NVARCHAR(100),
        popularity INT,
        song_added DATETIME2,
        album_id NVARCHAR(22),
        artist_id NVARCHAR(22)
    ) AS rows;
    ';

    -- Execute the dynamic SQL
    EXEC sp_executesql @sql_albums;
    EXEC sp_executesql @sql_artists;
    EXEC sp_executesql @sql_songs;
END;
GO

EXEC dbo.CreateSpotifyViews 
    @storage_account = 'your_storage_account',
    @storage_container = 'your_storage_container';

SELECT * FROM dbo.v_albums;
SELECT * FROM dbo.v_artists;
SELECT * FROM dbo.v_songs;