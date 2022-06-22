import requests
import json
from datetime import date
import mysql.connector
import pandas as pd
import numpy as np
import base64
from spotify_secret import *
from random_streams import generate_streaming_data


def get_token():
    url = 'https://accounts.spotify.com/api/token'
    headers = {}
    data = {'grant_type' : "client_credentials"}

    message = f"{clientId}:{clientSecret}"
    messageBytes = message.encode('ascii')
    base64Bytes = base64.b64encode(messageBytes)
    base64Message = base64Bytes.decode('ascii')

    headers['Authorization'] = f"Basic {base64Message}"
    r = requests.post(url, headers=headers, data=data)
    token = r.json()['access_token']
    return token


BASE_URL = 'https://api.spotify.com/v1/'
PLAYLIST_URI = '37i9dQZEVXbMDoHDwVN2tF'
URL = BASE_URL + 'playlists/' + PLAYLIST_URI +'/tracks'
TOKEN = get_token()


#laden der Daten von der Spotify API:

song_id = []
song_name = []
song_artist = []
song_duration_ms = []
song_album = []
song_number = []
song_explicit = []
song_popularity = []
album_id = []
album_name = []
album_release_date = []
album_total_tracks = []
artist_id = []

def spotify_etl():
    header = {
          "Accept" : "application/json",
          "Content-Type" : "application/json",
          "Authorization" : "Bearer {token}".format(token=TOKEN)
          }


    request = requests.get(URL, headers = header, params = {"limit" : 50})
    data = request.json()

    try:
        for song in data['items']:
            song_name.append(song["track"]["name"])
            song_artist.append(song["track"]["artists"][0]["name"])
            song_duration_ms.append(song["track"]["duration_ms"])
            song_popularity.append(song["track"]["popularity"])
            song_explicit.append(song["track"]["explicit"])
            song_id.append(song["track"]["id"])
            song_number.append(song["track"]["track_number"])
            album_id.append(song["track"]["album"]["id"])
            album_name.append(song["track"]["album"]["name"])
            album_release_date.append(song["track"]["album"]["release_date"])
            album_total_tracks.append(song["track"]["album"]["total_tracks"])
            artist_id.append(song["track"]["artists"][0]["id"])
    except KeyError:
        print("The token expired, generate a new one")


    song_dict = {"song_id" : song_id,
                 "song_name": song_name,
                 "artist_name": song_artist,
                 "artist_id" : artist_id,
                 "number_on_album": song_number,
                 "song_duration_ms" : song_duration_ms,
                 "popularity" : song_popularity,
                 "explicit" : song_explicit,
                 "album_id" : album_id,
                 "album_name" : album_name,
                 "album_release_date" : album_release_date,
                 "album_total_tracks" : album_total_tracks}


    song_df = pd.DataFrame(song_dict, columns = ["song_id", "song_name", "artist_name", "artist_id", "song_duration_ms", "popularity", "explicit",
                                                 "number_on_album", "album_id", "album_name", "album_release_date", "album_total_tracks"])
    
    # Gegenchecken der Daten:

    # Gibt es keine doppelten Werte in song_id?
    if len(song_df["song_id"].unique()) <  50:
        raise Exception("We have duplicate song ids")

    # Gibt es 50 Reihen und 12 Spalten?
    if song_df.shape != 50:
        raise Exception("The Data Frame has not got the right shape") 

    #Laden in den Datemart:
    
    db = mysql.connector.connect(
        user = db_user,
        password = db_password,
        host= db_host,
        database='Spotify_top_50_songs'
    )

    db_cursor = db.cursor()




# Ausführen der queries:

    SQL_0 = "TRUNCATE TABLE TEMP_SPOTIFY_CHARTS;"


    SQL_2 = """ INSERT INTO chart_songs (song_id,
                                        song_name,
                                        artist_id,
                                        album_id,
                                        song_duration_ms,
                                        explicit,
                                        song_popularity,
                                        entered_charts,
                                        days_in_charts,
                                        valid_from
                                        )
                SELECT song_id,
                       song_name,
                       artist_id,
                       album_id,
                       song_duration_ms,
                       explicit,
                       popularity,
                       curdate(),
                       1,
                       curdate()
                FROM TEMP_SPOTIFY_CHARTS
                ON DUPLICATE KEY UPDATE days_in_charts=days_in_charts+1"""


    SQL_3 = """ UPDATE Chart_songs
                SET left_charts =
                CASE WHEN song_id NOT IN (SELECT distinct song_id from TEMP_SPOTIFY_CHARTS)
                THEN  sysdate()
                ELSE  NULL END; """


    SQL_4 = """ INSERT INTO albums ( album_id,
                                     album_name,
                                     album_release_date,
                                     album_total_tracks,
                                     valid_to)
                SELECT DISTINCT album_id,
                       album_name,
                       album_release_date,
                       album_total_tracks,
                       curdate()
                FROM TEMP_SPOTIFY_CHARTS
                WHERE album_id NOT IN(
                SELECT album_id FROM albums
                )"""


    SQL_5 = """ INSERT INTO artists (artist_id, artist_name, valid_to)
                SELECT DISTINCT artist_id, artist_name, curdate()
                FROM TEMP_SPOTIFY_CHARTS
                WHERE artist_id NOT IN (
                SELECT artist_id FROM artists
                )"""

    SQL_6 = """ INSERT INTO streams_fakten( user_id,
    										song_id,
    										album_id,
                                            artist_id,
    										played_at_id
                                                )
    				SELECT s.user_id,
    					   t.song_id,
                           t.album_id,
                           t.artist_id,
                           d.date_id
    				FROM streams s
                    JOIN chart_songs t
                    ON s.song_id = t.song_id
                    JOIN dates d
                    ON d.year = SUBSTR(s.played_at, 1,4)
                    AND d.month = SUBSTR(s.played_at, 6,2)
                    AND d.day_numerical = SUBSTR(s.played_at, 9,2)
                    AND d.hour = SUBSTR(s.played_at, 12,2)"""

    SQL_7 = """ INSERT INTO dates(
                year,
                month,
                day_numerical,
                hour)
                SELECT DISTINCT
                SUBSTRING(played_at, 1,4),
                SUBSTRING(played_at,6,2),
                SUBSTRING(played_at,9,2),
                SUBSTRING(played_at,12,2)
                FROM streams"""

    try:
        db_cursor.execute(SQL_0)
        print("Temp table truncated")
        db.commit()
    except:
        print("Temp table could not be truncated")


    cols = "`,`".join([str(i) for i in song_df.columns.tolist()])
    try:
        for i,row in song_df.iterrows():
            SQL_1 = "INSERT INTO `TEMP_SPOTIFY_CHARTS` (`" +cols + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
            db_cursor.execute(SQL_1, tuple(row))
            db.commit()
        print("Inserted rows into temp table")
    except:
        print("Rows could not be inserted")

    # Generieren der fiktiven Streaming Daten basierend auf
    # den Songs in den Charts:

    generate_streaming_data()


    try:
        db_cursor.execute(SQL_4)
        db.commit()
        print("New albums inserted")
    except:
        print("New albums could not be inserted")


    try:
        db_cursor.execute(SQL_5)
        db.commit()
        print("New artists inserted")
    except:
        print("New artists coul not be inserted")


    try:
        db_cursor.execute(SQL_2)
        db.commit()
        print("New songs inserted / old ones updated")
    except:
        print("News songs could not be inserted / old ones updated")


    try:
        db_cursor.execute(SQL_3)
        db.commit()
        print("Inserted values for left_charts for the relevant songs")
    except:
        print("values for left_charts for the relevant songs could not be inserted")

    try:
        db_cursor.execute(SQL_7)
        db.commit()
        print("Inserted Values in the date table")
    except:
        print("Values could not be inserted in the date table")

    try:
        db_cursor.execute(SQL_6)
        db.commit()
        print("Inserted Values in the fact table")
    except:
        print("Values could not be inserted in the fact table")

spotify_etl()
