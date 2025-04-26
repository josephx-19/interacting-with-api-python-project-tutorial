import os
from dotenv import load_dotenv
import sqlite3
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from spotipy import Spotify
from spotipy.oauth2 import SpotifyClientCredentials
from spotipy.exceptions import SpotifyException

# ----------------------------------------
# Carga de variables de entorno desde .env
# ----------------------------------------
load_dotenv()
SPOTIPY_CLIENT_ID = os.getenv('SPOTIPY_CLIENT_ID')
SPOTIPY_CLIENT_SECRET = os.getenv('SPOTIPY_CLIENT_SECRET')
if not SPOTIPY_CLIENT_ID or not SPOTIPY_CLIENT_SECRET:
    raise EnvironmentError("Define SPOTIPY_CLIENT_ID y SPOTIPY_CLIENT_SECRET en las variables de entorno.")

# ----------------------------------------
# Configuración del cliente de Spotify
# ----------------------------------------
credentials = SpotifyClientCredentials(
    client_id=SPOTIPY_CLIENT_ID,
    client_secret=SPOTIPY_CLIENT_SECRET
)
sp = Spotify(client_credentials_manager=credentials)

# ----------------------------------------
# Funciones auxiliares
# ----------------------------------------

def fetch_artist_tracks(artist_id: str, country: str = "US") -> pd.DataFrame:
    """
    Descarga las top tracks de un artista y devuelve un DataFrame.
    """
    try:
        data = sp.artist_top_tracks(artist_id, country=country)
    except SpotifyException as e:
        if e.http_status == 404:
            raise ValueError(f"Artista no encontrado: {artist_id}")
        else:
            raise

    tracks = data.get('tracks', [])
    if not tracks:
        print(f"⚠️ No se encontraron pistas para el artista {artist_id}.")
        return pd.DataFrame()

    records = []
    for t in tracks:
        records.append({
            'track_id': t.get('id'),
            'name': t.get('name'),
            'artist': t.get('artists')[0].get('name') if t.get('artists') else None,
            'album': t.get('album').get('name'),
            'release_date': t.get('album').get('release_date'),
            'popularity': t.get('popularity'),
            'duration_ms': t.get('duration_ms')
        })
    df = pd.DataFrame(records)
    # Convertir release_date a datetime
    df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce')
    # Convertir duration de ms a minutos
    df['duration_min'] = df['duration_ms'] / 60000
    return df

def save_to_db(df: pd.DataFrame, db_path: str = 'spotify_data.db', table_name: str = 'artist_tracks') -> None:
    """
    Guarda el DataFrame en SQLite.
    """
    conn = sqlite3.connect(db_path)
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()

def load_from_db(db_path: str = 'spotify_data.db', table_name: str = 'artist_tracks') -> pd.DataFrame:
    """
    Carga datos desde SQLite a un DataFrame.
    """
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
    conn.close()
    return df

# ----------------------------------------
# Script principal
# ----------------------------------------

def main():
    # Usa un ARTIST ID válido
    artist_id = os.getenv('SPOTIFY_ARTIST_ID', '66q6iqbR9rh3jJNlGEnQvB')

    # Descargar datos
    try:
        df = fetch_artist_tracks(artist_id)
        if df.empty:
            return
        print(f"✅ Descargadas {len(df)} pistas para el artista {artist_id}")
    except ValueError as e:
        print(e)
        return

    # Guardar en SQLite
    save_to_db(df)
    print("💾 Datos guardados en spotify_data.db -> tabla artist_tracks")

    # Leer y mostrar los datos guardados
    df_db = load_from_db()
    print(df_db.head())

    # Analizar relación duración-popularidad
    plt.figure(figsize=(10, 6))
    sns.scatterplot(x='duration_min', y='popularity', data=df_db)
    plt.xlabel('Duration (minutes)')
    plt.ylabel('Popularity')
    plt.title('Relationship between Duration and Popularity')
    plt.grid(True)
    plt.show()

if __name__ == '__main__':
    main()
