# spotify-etl

### Verwendete Technologien:
- Python
- Spotify API
- MySQL
- Apache Airflow
- Dimensionale Modellierung

### Beschreibung:

In den folgenden Skripten habe ich einen Datamart, der Streaming Daten zu den aktuellen Spotify Charts enthält mit einem ETL Skript befüllt. Die Daten zu den Songs in den Charts werden über die Spotify API extrahiert. Daten zu den genauen Klicks und Benutzern die sie verursachen werden von Spotify aus Datenschutzgründen nicht bereitgestellt. Aus diesem Grund habe ich die Benutzer per Zufall generiert (Benutzerdaten wie Geschlecht entsprechen der tatsächlichen Verteilung der Spotify Nutzer). Diesen ordne ich dann in einer Tabelle per Zufall Lieder aus den Charts und die Uhrzeit sowie das Datum, an dem sie diese abgespielt haben zu und betrachte diese Tabelle als eine Datenquelle. 

In meinem Skript selektiere ich dann die Streaming Daten aus der Datenbank und der Spotify API, mache einige Gegenchecks und lade sie in diese Datamart-Struktur im Stern-Schema:


Der gesamte ETL-Prozess wird einmal täglich automatisch von Apache Airflow ausgeführt.
Wenn ein bestimmter Song einen weiteren Tag in den Charts ist, dann wird days_in_charts um 1 erhöht, wenn er nicht mehr drinnen ist, dann wird left_charts auf das aktuelle Datum gesetzt. Die Benutzer bleiben in diesem Fall konstant, da sie ja von mir per Zufall generiert wurden. In der Realität würde man aus einer Common Core Tabelle all die Nutzer laden, die in den letzten 24h einen der Songs in den Charts angehört haben.
Der ETL-Prozess visuell dargestellt:


