
from __future__ import annotations
from typing import Tuple
import datetime
import calendar

import polars as pl
import altair as alt
alt.data_transformers.enable("vegafusion")
import plotly
import plotly.express as px

from spotify_analysis.src.data.streaming_history import StreamingHistory

def get_wrapped_range(year: int) -> pl.Expr:
    return (
        (datetime.date(year=year,month=1,day=1) <= pl.col("date"))
        & (pl.col("date") <= datetime.date(year=year,month=10,day=31))
    )


class StreamingHistoryAnalyser:
    def __init__(self, SteamHistory: StreamingHistory):
        self._stream_history = SteamHistory
        self.cleaned_data: pl.DataFrame = SteamHistory.cleaned_data
        self.years = (self.cleaned_data["ts"].dt.year().unique().to_list())
        self.min_year: int = min(self.years)
        self.max_year: int = max(self.years)
    
    def get_cleaned_data(self, year: int) -> pl.DataFrame:
        if isinstance(year, int):
            return (
                self.cleaned_data
                .with_columns(date=pl.col("ts").dt.date())
                .filter(get_wrapped_range(year))
            )
        else:
            return self.cleaned_data
    
    def get_total_mins_played(self, year: int = None) -> float:
        return self.get_cleaned_data(year)["mins_played"].sum()
    
    def get_total_hours_played(self, year: int = None) -> float:
        return self.get_total_mins_played(year) / 60
    
    def get_total_days_played(self, year: int = None) -> int:
        return self.get_total_hours_played(year) / 24
    
    def get_total_tracks_played(self, year: int = None) -> int:
        return self.get_cleaned_data(year).shape[0]
    
    def get_total_days_played(self, year: int = None) -> int:
        return (len(self.get_cleaned_data(year)["ts"].dt.date().unique()))
    
    def get_total_days_covered(self, year: int = None) -> int:
        if year is None:
            min_date = self.get_cleaned_data(None)["ts"].dt.date().min()
            max_date = self.get_cleaned_data(None)["ts"].dt.date().max()
            return (max_date - min_date).days + 1
        else:
            return 365 + calendar.isleap(year)
    
    def get_avg_time_played_per_day(self, year: int = None) -> float:
        return self.get_total_mins_played(year) / self.get_total_days_covered(year)
    
    def get_avg_time_played_per_track(self, year: int = None) -> float:
        return self.get_total_mins_played(year) / self.get_total_tracks_played(year)
    
    def get_avg_tracks_played_per_day(self, year: int = None) -> float:
        return self.get_total_tracks_played(year) / self.get_total_days_covered(year)
    
    def get_num_unique_songs(self, year: int = None) -> int:
        return (
            self.get_cleaned_data(year)
            .group_by(["master_metadata_album_artist_name", "master_metadata_track_name"])
            .agg(pl.count("master_metadata_track_name").alias("num_plays"))
            .shape[0]
        )
    
    def get_num_unique_artists(self, year: int = None) -> int:
        return self.get_cleaned_data(year)["master_metadata_album_artist_name"].n_unique()
    
    def get_num_unique_albums(self, year: int = None) -> int:
        return (
            self.get_cleaned_data(year)
            .group_by(["master_metadata_album_artist_name", "master_metadata_album_album_name"])
            .agg(pl.count("master_metadata_album_album_name").alias("num_plays"))
            .shape[0]
        )
    
    def get_daily_play_counts(self, year: int = None) -> pl.DataFrame:
        return (
            self.get_cleaned_data(year)
            .select([
                pl.col("ts").dt.date().alias("date"),
                pl.col("mins_played"),
            ])
            .group_by("date")
            .agg(
                pl.col("mins_played").sum().alias("total_mins_played"),
                pl.col("mins_played").len().alias("num_plays"),
            )
            .with_columns(
                year=pl.col("date").dt.year().cast(str),
            )
            .sort("date")
        )
    
    def get_daily_artist_play_counts(self, year: int = None) -> pl.DataFrame:
        return (
            self.get_cleaned_data(year)
            .select([
                pl.col("ts").dt.date().alias("date"),
                pl.col("mins_played"),
                pl.col("master_metadata_album_artist_name"),
            ])
            .group_by(["date", "master_metadata_album_artist_name"])
            .agg(
                pl.col("mins_played").sum().alias("total_mins_played"),
                pl.col("mins_played").len().alias("num_plays"),
            )
            .sort(["date", "master_metadata_album_artist_name"])
        )
    
    def get_top_artists(self, year: int = None) -> pl.DataFrame:
        return (
            self.get_daily_artist_play_counts(year)
            .group_by("master_metadata_album_artist_name")
            .agg(
                pl.col("total_mins_played").sum().alias("total_mins_played"),
                pl.col("num_plays").sum().alias("num_plays"),
            )
            .sort("total_mins_played", descending=True)
        )
    
    def get_daily_song_play_counts(self, year: int = None) -> pl.DataFrame:
        return (
            self.get_cleaned_data(year)
            .select([
                pl.col("ts").dt.date().alias("date"),
                pl.col("mins_played"),
                pl.col("master_metadata_track_name"),
                pl.col("master_metadata_album_artist_name")
            ])
            .group_by(
                [
                    "date",
                    "master_metadata_album_artist_name",
                    "master_metadata_track_name",
                ]
            )
            .agg(
                pl.col("mins_played").sum().alias("total_mins_played"),
                pl.len().alias("num_plays"),
            )
            .sort(
                by=[
                    "date",
                    "master_metadata_album_artist_name",
                    "master_metadata_track_name",
                ]
            )
        )
    
    def get_song_total_plays(self, year: int = None) -> pl.DataFrame:
        return (
            self.get_daily_song_play_counts(year)
            .group_by(
                [
                    "master_metadata_album_artist_name",
                    "master_metadata_track_name",
                ]
            )
            .agg(
                pl.col("total_mins_played").sum().alias("total_mins_played"),
                pl.col("num_plays").sum().alias("total_num_plays"),
            )
            .sort("total_num_plays", descending=True)
            # .drop_nulls()
        )
    
    def get_daily_mins_played_chart(self, year: int = None) -> plotly.graph_objects.Figure:
        return (
            px.scatter(
                self.get_daily_play_counts(year),
                x="date",
                y="total_mins_played",
                color="year",
                title="Minutes played over time",
                trendline="lowess",
                trendline_color_override="white",
            )
            .update_traces(marker=dict(size=4))
        )
    
    def get_top_artists_bar_chart(
        self,
        year: int = None,
        num_artists: int = 20,
    ) -> alt.Chart:
        return (
            alt.Chart(self.get_top_artists(year).head(num_artists))
            .mark_bar()
            .encode(
                x=alt.X(
                    "master_metadata_album_artist_name:N",
                    title="Artist",
                    sort=alt.EncodingSortField(field="total_mins_played",
                    op="sum",
                    order="descending"),
                ),
                y=alt.Y(
                    "total_mins_played:Q",
                    title="Total Minutes Played",
                ),
                color=alt.Color("num_plays:Q", title="Number of Plays"),
                tooltip=[
                    alt.Tooltip("master_metadata_album_artist_name:N", title="Artist"),
                    alt.Tooltip("total_mins_played:Q", title="Total Minutes Played", format=",.1f"),
                    alt.Tooltip("num_plays:Q", title="Number of Plays", format=",.0f"),
                ]
            )
            .properties(
                width=1200,
                height=500,
            )
        )
    
    def get_top_songs_cumulative_plays_chart(
        self,
        year: int = None,
        num_songs: int = None,
    ) -> alt.Chart:
        song_total_plays = (
            self.get_daily_song_play_counts(year)
            .group_by("master_metadata_album_artist_name", "master_metadata_track_name")
            .agg(pl.col("total_mins_played").sum().alias("total_mins_played"), pl.col("num_plays").sum().alias("total_num_plays"))
            .sort("total_num_plays", descending=True)
            # .drop_nulls()
        )
        top_songs_listens = (
            self.get_daily_song_play_counts(year)
            .join(
                (
                    song_total_plays
                    [["master_metadata_album_artist_name","master_metadata_track_name"]]
                    .head(num_songs)
                ),
                on=["master_metadata_album_artist_name","master_metadata_track_name"],
                how="inner",
            )
            .with_columns(
                cumsum_total_mins_played=pl.col("total_mins_played").cum_sum().over(["master_metadata_album_artist_name","master_metadata_track_name"]),
                cumsum_num_plays=pl.col("num_plays").cum_sum().over(["master_metadata_album_artist_name","master_metadata_track_name"]),
                song_name=pl.col("master_metadata_album_artist_name") + " - " + pl.col("master_metadata_track_name"),
            )
        )
        colour_scheme = "category10" if num_songs <= 10 else "category20"
        return alt.Chart(top_songs_listens).mark_line(point=True).encode(
            x=alt.X(
                "date:T",
                title="Date",
            ),
            y=alt.Y(
                "cumsum_num_plays:Q",
                title="Cumulative Number of Plays",
            ),
            color=alt.Color(
                "song_name:N",
                title="Artist",
                scale=alt.Scale(scheme=colour_scheme),
                sort=alt.EncodingSortField(
                    field="cumsum_num_plays",
                    op="max",
                    order="descending",
                ),
            ),
            tooltip=[
                alt.Tooltip("date:T", title="Date"),
                alt.Tooltip("cumsum_total_mins_played:Q", title="Cumulative Total Minutes Played", format=",.1f"),
                alt.Tooltip("cumsum_num_plays:Q", title="Cumulative Number of Plays", format=",.0f"),
                alt.Tooltip("master_metadata_album_artist_name:N", title="Artist"),
                alt.Tooltip("master_metadata_track_name:N", title="Song"),
            ],
        ).properties(
            title=f"Cumulative Total Minutes Played Over Time for Top {num_songs} Songs",
            width=1200,
            height=600,
        )
    
    def get_hyperfixation_songs(self, year: int = None, n_days: int = 7) -> pl.DataFrame:
        return (
            self.get_daily_song_play_counts(year)
            .with_columns(
                cumsum_total_mins_played=(
                    pl.col("total_mins_played")
                    .rolling_sum_by("date", window_size=datetime.timedelta(days=n_days))
                    .over(["master_metadata_album_artist_name","master_metadata_track_name"])
                ),
                cumsum_num_plays=(
                    pl.col("num_plays")
                    .rolling_sum_by("date", window_size=datetime.timedelta(days=n_days))
                    .over(["master_metadata_album_artist_name","master_metadata_track_name"])
                ),
            )
            .group_by(["master_metadata_album_artist_name","master_metadata_track_name"])
            .agg(
                pl.col("cumsum_total_mins_played").max().alias("max_cumsum_total_mins_played"),
                pl.col("cumsum_num_plays").max().alias("max_cumsum_num_plays"),
            )
            .filter(pl.col("max_cumsum_num_plays") > 2)
            .filter(pl.col("max_cumsum_total_mins_played") > 0)
            .sort("max_cumsum_total_mins_played", descending=True)
            # .drop_nulls()
        )