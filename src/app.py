import os
import sqlite3
import pandas as pd
from spotipy import Spotify
from spotipy.oauth2 import SpotifyClientCredentials

# Cargar credenciales de Spotify desde variables de entorno
SPOTIPY_CLIENT_ID = os.getenv('SPOTIPY_CLIENT_ID')
SPOTIPY_CLIENT_SECRET = os.getenv('SPOTIPY_CLIENT_SECRET')
if not SPOTIPY_CLIENT_ID or not SPOTIPY_CLIENT_SECRET:
    raise EnvironmentError("Define SPOTIPY_CLIENT_ID y SPOTIPY_CLIENT_SECRET en las variables de entorno.")

# Inicializar cliente de Spotify
credentials = SpotifyClientCredentials(client_id=SPOTIPY_CLIENT_ID,
                                       client_secret=SPOTIPY_CLIENT_SECRET)
sp = Spotify(client_credentials_manager=credentials)

# Función para obtener pistas de una playlist
def fetch_playlist_tracks(playlist_id, limit=100):
    results = sp.playlist_items(playlist_id, limit=limit)
    items = results['items']
    # Extraer campos relevantes
    records = []
    for item in items:
        track = item['track']
        records.append({
            'track_id': track['id'],
            'name': track['name'],
            'artist': track['artists'][0]['name'],
            'album': track['album']['name'],
            'release_date': track['album']['release_date'],
            'popularity': track['popularity']
        })
    return pd.DataFrame(records)


def main():
    # Ejemplo: top 50 global
    playlist_id = '37i9dQZEVXbMDoHDwVN2tF'  # Spotify Global Top 50
    df = fetch_playlist_tracks(playlist_id)
    print(f"Descargadas {len(df)} pistas")

    # Conectar SQLite
    conn = sqlite3.connect('spotify_data.db')
    # Guardar en SQL
    df.to_sql('playlist_tracks', conn, if_exists='replace', index=False)
    conn.close()
    print("Datos guardados en spotify_data.db -> tabla playlist_tracks")

if __name__ == '__main__':
    main()
