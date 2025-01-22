
from typing import Dict

import polars as pl

streaming_history_audio_schema: Dict[str, pl.DataType] = {
    "ts": pl.Datetime,
    "username": pl.Utf8,
    "platform": pl.Utf8,
    "ms_played": pl.Int64,
    "conn_country": pl.Utf8,
    "ip_addr_decrypted": pl.Utf8,
    "user_agent_decrypted": pl.Utf8,
    "master_metadata_track_name": pl.Utf8,
    "master_metadata_album_artist_name": pl.Utf8,
    "master_metadata_album_album_name": pl.Utf8,
    "spotify_track_uri": pl.Utf8,
    "episode_name": pl.Utf8,
    "episode_show_name": pl.Utf8,
    "spotify_episode_uri": pl.Utf8,
    "reason_start": pl.Utf8,
    "reason_end": pl.Utf8,
    "shuffle": pl.Boolean,
    "skipped": pl.Boolean,
    "offline": pl.Boolean,
    "offline_timestamp": pl.Int64,
    "incognito_mode": pl.Boolean
}