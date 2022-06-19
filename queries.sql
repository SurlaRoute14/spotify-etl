TRUNCATE TABLE TEMP_SPOTIFY_CHARTS;

-- Laden der songs aus der temp Tabelle in die songs Tabelle:
-- Falls der Song schon vorhanden ist, wird lediglich days_in_charts um einen Tag erhöht.

INSERT INTO chart_songs (song_id,
                         song_name,
                         artist_id,
                         album_id,
                         song_duration_ms,
			 explicit,
                         song_popularity,
                         entered_charts,
                         days_in_charts,
                         valid_from)
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
ON DUPLICATE KEY UPDATE days_in_charts=days_in_charts+1;

-- Setzten der left_chart Spalte für Songs die nicht mehr in den Charts sind:

UPDATE Chart_songs
SET left_charts =
    CASE WHEN song_id
         NOT IN (
         SELECT distinct song_id
         FROM TEMP_SPOTIFY_CHARTS)
	THEN  sysdate()
    ELSE  NULL 
END;

-- Einfügen der Alben die noch nicht vorhanden sind:


INSERT INTO albums ( 
		    album_id,
            album_name,
            album_release_date,
            album_total_tracks,
            valid_from)
SELECT DISTINCT album_id,
                album_name,
                album_release_date,
                album_total_tracks,
                curdate()
FROM TEMP_SPOTIFY_CHARTS
WHERE album_id NOT IN(
SELECT album_id FROM albums);

-- Einfügen der Künstler die noch nicht vorhanden sind:

INSERT INTO artists (
		    artist_id, 
            artist_name,
            valid_from)
SELECT DISTINCT artist_id, 
		artist_name, 
		cordate()
FROM TEMP_SPOTIFY_CHARTS
WHERE artist_id NOT IN (
      SELECT artist_id
      FROM artists);

-- Befüllen der Faktentabelle mit den Fremdschlüsseln:

INSERT INTO streams_fakten( user_id,
			    song_id,
			    album_id,
			    artist_id,
			    played_at_id)
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
AND d.hour = SUBSTR(s.played_at, 12,2);

-- Befüllen der datums Tabelle

INSERT INTO dates( year,
                   month,
                   day_numerical,
                   hour)
SELECT DISTINCT SUBSTRING(played_at, 1,4),
                SUBSTRING(played_at,6,2),
                SUBSTRING(played_at,9,2),
                SUBSTRING(played_at,12,2)
FROM streams;
      




