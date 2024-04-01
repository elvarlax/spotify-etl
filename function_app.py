import os
import logging
import json
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import azure.functions as func
from azure.storage.filedatalake import DataLakeServiceClient
from azure.storage.blob import BlobServiceClient
from datetime import datetime
from io import StringIO
import pandas as pd

app = func.FunctionApp()

# Retrieve environment variables
spotify_client_id = os.environ.get("SPOTIFY_CLIENT_ID")
spotify_client_secret = os.environ.get("SPOTIFY_CLIENT_SECRET")
storage_account_name = os.environ.get("STORAGE_ACCOUNT_NAME")
storage_account_key = os.environ.get("STORAGE_ACCOUNT_KEY")
storage_connection_string = os.environ.get("STORAGE_CONNECTION_STRING")
storage_container = os.environ.get("STORAGE_CONTAINER")


@app.timer_trigger(
    arg_name="myTimer",
    schedule="0 * * * *"
)
def spotify_data_extract(myTimer: func.TimerRequest) -> None:
    """Extracts data from the Spotify API and stores it in Azure Data Lake Storage Gen2.

    Args:
        myTimer (func.TimerRequest): The timer trigger object.

    Returns:
        None
    """
    try:
        # Initialize Spotify client
        credentials = SpotifyClientCredentials(client_id=spotify_client_id, client_secret=spotify_client_secret)
        spotify = spotipy.Spotify(client_credentials_manager=credentials)

        # Fetch data from Spotify playlist
        playlist_id = "37i9dQZEVXbNG2KDcFcKOF"
        spotify_data = spotify.playlist_tracks(playlist_id)

        # Initialize ADLS Gen2 client and get file system client
        account_url = f"https://{storage_account_name}.dfs.core.windows.net"
        service_client = DataLakeServiceClient(account_url=account_url, credential=storage_account_key)
        file_system_client = service_client.get_file_system_client(file_system=storage_container)

        # Create or get file client
        timestamp = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
        file_name = f"raw_data/to_processed/spotify_data_{timestamp}.json"
        file_client = file_system_client.get_file_client(file_name)

        # Upload the data
        file_client.upload_data(json.dumps(spotify_data), overwrite=True)

        logging.info(f"Successfully uploaded data to {file_name}")

        return None
    except Exception as e:
        logging.exception(f"An error occurred during data extraction: {str(e)}")


@app.blob_trigger(
    arg_name="myblob",
    path=storage_container + "/raw_data/to_processed/{name}.json",
    connection="STORAGE_CONNECTION_STRING"
)
def spotify_data_transformation_load(myblob: func.InputStream):
    spotify_data = process_blobs(storage_connection_string, storage_container)
    album_df, artist_df, song_df = transform_data(spotify_data)
    """Transforms and loads Spotify data stored in Azure Data Lake Storage Gen2.

    Args:
        myblob (func.InputStream): The input blob trigger.

    Returns:
        None
    """

    # Initialize ADLS Gen2 client and get file system client
    account_url = f"https://{storage_account_name}.dfs.core.windows.net"

    # Directly use the storage account key for authentication
    service_client = DataLakeServiceClient(account_url=account_url, credential=storage_account_key)
    file_system_client = service_client.get_file_system_client(file_system=storage_container)

    # Format timestamp for filename
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    # Upload songs, album, artists to transformed_data directories
    load_dataframe_to_adls(song_df, file_system_client, f"transformed_data/songs_data/song_transformed_{timestamp}.csv")
    load_dataframe_to_adls(album_df, file_system_client, f"transformed_data/album_data/album_transformed_{timestamp}.csv")
    load_dataframe_to_adls(artist_df, file_system_client, f"transformed_data/artist_data/artist_transformed_{timestamp}.csv")

    # Move the raw_data from to_processed to processed
    copy_files_and_cleanup(file_system_client, "raw_data/to_processed", "raw_data/processed")


def process_blobs(storage_connection_string, storage_container):
    """Processes Spotify data blobs stored in Azure Data Lake Storage Gen2.

    Args:
        storage_connection_string (str): The connection string to the storage account.
        storage_container (str): The name of the storage container.

    Returns:
        list: A list of Spotify data extracted from blobs.
    """
    blob_service_client = BlobServiceClient.from_connection_string(conn_str=storage_connection_string)
    container_client = blob_service_client.get_container_client(container=storage_container)
    file_prefix = "/raw_data/to_processed/spotify_data"
    spotify_data = []

    for blob in container_client.list_blobs(name_starts_with=file_prefix):
        if blob.name.endswith(".json"):
            logging.info(f"Processing {blob.name}")

            # Retrieve the blob's content
            blob_client = container_client.get_blob_client(blob.name)
            blob_content = blob_client.download_blob().readall()
            data = json.loads(blob_content)

            # Append the data to the spotify_data list
            spotify_data.append(data)
    return spotify_data


def album(data):
    """Extracts album information from Spotify data.

    Args:
        data (dict): Spotify data.

    Returns:
        list: A list of dictionaries containing album information.
    """
    album_list = []
    for row in data["items"]:
        album_id = row["track"]["album"]["id"]
        album_name = row["track"]["album"]["name"]
        album_release_date = row["track"]["album"]["release_date"]
        album_total_tracks = row["track"]["album"]["total_tracks"]
        album_url = row["track"]["album"]["external_urls"]["spotify"]
        album_element = {
            "album_id": album_id,
            "name": album_name,
            "release_date": album_release_date,
            "total_tracks": album_total_tracks,
            "url": album_url,
        }
        album_list.append(album_element)
    return album_list


def artist(data):
    """Extracts artist information from Spotify data.

    Args:
        data (dict): Spotify data.

    Returns:
        list: A list of dictionaries containing artist information.
    """
    artist_list = []
    for row in data["items"]:
        for key, value in row.items():
            if key == "track":
                for artist in value["artists"]:
                    artist_dict = {
                        "artist_id": artist["id"],
                        "artist_name": artist["name"],
                        "external_url": artist["href"],
                    }
                    artist_list.append(artist_dict)
    return artist_list


def songs(data):
    """Extracts song information from Spotify data.

    Args:
        data (dict): Spotify data.

    Returns:
        list: A list of dictionaries containing song information.
    """
    song_list = []
    for row in data["items"]:
        song_id = row["track"]["id"]
        song_name = row["track"]["name"]
        song_duration = row["track"]["duration_ms"]
        song_url = row["track"]["external_urls"]["spotify"]
        song_popularity = row["track"]["popularity"]
        song_added = row["added_at"]
        album_id = row["track"]["album"]["id"]
        artist_id = row["track"]["album"]["artists"][0]["id"]
        song_element = {
            "song_id": song_id,
            "song_name": song_name,
            "duration_ms": song_duration,
            "url": song_url,
            "popularity": song_popularity,
            "song_added": song_added,
            "album_id": album_id,
            "artist_id": artist_id,
        }
        song_list.append(song_element)
    return song_list


def transform_data(spotify_data):
    """Transforms Spotify data into DataFrames.

    Args:
        spotify_data (list): A list of Spotify data.

    Returns:
        tuple: Three DataFrames containing transformed album, artist, and song data.
    """
    all_albums = []
    all_artists = []
    all_songs = []

    # Process each data item
    for data in spotify_data:
        all_albums.extend(album(data))
        all_artists.extend(artist(data))
        all_songs.extend(songs(data))

    # Convert lists to DataFrames and remove duplicates
    album_df = pd.DataFrame(all_albums).drop_duplicates(subset=["album_id"])
    artist_df = pd.DataFrame(all_artists).drop_duplicates(subset=["artist_id"])
    song_df = pd.DataFrame(all_songs)

    # Convert date columns to datetime format
    album_df["release_date"] = pd.to_datetime(album_df["release_date"])
    song_df["song_added"] = pd.to_datetime(song_df["song_added"])

    return album_df, artist_df, song_df


def load_dataframe_to_adls(df, file_system_client, file_path):
    """Uploads a DataFrame to Azure Data Lake Storage Gen2.

    Args:
        df (DataFrame): The DataFrame to upload.
        file_system_client: The Azure Data Lake Storage Gen2 file system client.
        file_path (str): The path where the DataFrame should be stored.

    Returns:
        None
    """
    csv_output = StringIO()
    df.to_csv(csv_output, index=False)
    file_client = file_system_client.get_file_client(file_path)
    file_client.upload_data(csv_output.getvalue(), overwrite=True)
    csv_output.close()


def copy_files_and_cleanup(file_system_client, source_directory, destination_directory):
    """Copies files from one directory to another and cleans up the source directory.

    Args:
        file_system_client: The Azure Data Lake Storage Gen2 file system client.
        source_directory (str): The source directory.
        destination_directory (str): The destination directory.

    Returns:
        None
    """
    paths = file_system_client.get_paths(path=source_directory, recursive=True)

    # Copy files from source to destination
    for path in paths:
        if not path.is_directory:
            # Construct the new path by replacing source directory with destination directory in the file's path
            new_path = path.name.replace(source_directory, destination_directory, 1)

            # Copy the file to the new location
            file_client = file_system_client.get_file_client(path.name)
            new_file_client = file_system_client.get_file_client(new_path)
            new_file_client.create_file()
            stream = file_client.download_file()
            new_file_client.append_data(stream.readall(), 0)
            new_file_client.flush_data(stream.size)

            # Delete the original file
            file_client.delete_file()

    # Attempt to delete the source directory (and any empty subdirectories)
    try:
        file_system_client.get_directory_client(source_directory).delete_directory(recursive=True)
    except Exception as e:
        logging.info(f"Could not delete directory {source_directory}: {str(e)}")
