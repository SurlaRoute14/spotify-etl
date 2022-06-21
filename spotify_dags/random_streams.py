import random
import mysql.connector
import pandas as pd
import numpy as np
import re
import datetime

# Hier ordne ich die gestreamten Songs den von mir per Zufallsprinzip erstellten Benutzern in der users Tabelle per Zufall zu. 
# Dadurch habe ich eine Grundlage für meine Fakten Tabelle. 

def generate_streaming_data():

    today = str(datetime.date.today())
    tomorrow = str(datetime.date.today() + datetime.timedelta(days=1))
    dates = pd.date_range(start=today, end = tomorrow , freq='H')

    db = mysql.connector.connect(
        user = db_user,
        password = db_password,
        host= db_host,
        database='Spotify_top_50_songs'
        )

    db_cursor = db.cursor()

    SQL = """SELECT distinct song_id
             FROM TEMP_SPOTIFY_CHARTS"""

    SQL_2 = """SELECT distinct user_id
               FROM users"""

    SQL_3 = """ TRUNCATE TABLE streams """

    db_cursor.execute(SQL_3)
    db.commit()

    db_cursor.execute(SQL)

    song_id = []
    for row in db_cursor:
        row = str(row)
        row = row[2:-3]
        song_id.append(row)

    db_cursor.execute(SQL_2)
    user_id = []
    for row in db_cursor:
        row = re.findall('[0-9]+', str(row))
        user_id.append(row[0])

    hours_id = []

    for row in dates:
        hours_id.append(str(row))

    user = list(random.choices(user_id, k=500))
    song = list(random.choices(song_id, k=500))
    playtime = list(random.choices(hours_id, k=500))

    streams_df = pd.DataFrame(zip(user, song, playtime), columns=["user_id", "song_id", "played_at"])

    cols = "`,`".join([str(i) for i in streams_df.columns.tolist()])
    for i,row in streams_df.iterrows():
        SQL_1 = "INSERT INTO `streams` (`" +cols + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
        db_cursor.execute(SQL_1, tuple(row))
        db.commit()

generate_streaming_data()
