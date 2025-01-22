from __future__ import annotations
from typing import List
from pathlib import Path
import zipfile

import polars as pl

from spotify_analysis.src.data._schema import streaming_history_audio_schema

class StreamingHistory:
    def __init__(self, zip_path: Path) -> None:
        self._zip_path = zip_path
        self._raw_data: pl.DataFrame = None
        self._cleaned_data: pl.DataFrame = None

    def read_data(self) -> StreamingHistory:
        dfs: List[pl.DataFrame] = []
        with zipfile.ZipFile(self._zip_path, 'r') as zip_ref:
            for file_name in zip_ref.namelist():
                if (
                    file_name.startswith("Spotify Extended Streaming History/Streaming_History_Audio_")
                    and file_name.endswith(".json")
                ):
                    with zip_ref.open(file_name) as file:
                        df = pl.read_json(file, schema=streaming_history_audio_schema)
                        dfs.append(df)
        
        self._raw_data: pl.DataFrame = (
            pl.concat(dfs)
            .sort("ts")
            .with_columns(
                mins_played=pl.col("ms_played") / 1_000 / 60,
            )
        )
        return self
    
    def clean_data(self) -> StreamingHistory:
        self._cleaned_data: pl.DataFrame = (
            self._raw_data
            .with_columns(
                pl.when((~pl.col("offline")) | (pl.col("offline_timestamp") == 1))
                .then(pl.col("ts"))
                .when(pl.col("offline_timestamp") < 10_000_000_000)
                .then(pl.col("offline_timestamp").mul(1_000_000).cast(pl.Datetime))
                .otherwise(pl.col("offline_timestamp").mul(1_000).cast(pl.Datetime))
                .alias("ts")
            )
            .select(
                [
                    pl.col("ts"),
                    pl.col("mins_played"),
                    pl.col("master_metadata_track_name"),
                    pl.col("master_metadata_album_artist_name"),
                    pl.col("master_metadata_album_album_name"),
                    pl.col("reason_start"),
                    pl.col("reason_end"),
                    pl.col("shuffle"),
                    pl.col("skipped"),
                    ~pl.col("skipped").fill_null(False).alias("played"),
                    pl.col("offline"),
                    pl.col("incognito_mode"),
                    pl.col("platform"),
                ],
            )
            .filter(
                (pl.col("mins_played") > 1) | pl.col("played")
            )
            .drop_nulls(
                subset=[
                    "ts",
                    "mins_played",
                    "master_metadata_track_name",
                    "master_metadata_album_artist_name",
                    "master_metadata_album_album_name",
                ],
            )
        )
        return self
    
    @property
    def cleaned_data(self) -> pl.DataFrame:
        if self._cleaned_data is None:
            raise ValueError("Data has not been cleaned yet. Call clean_data() first.")
        return self._cleaned_data
    