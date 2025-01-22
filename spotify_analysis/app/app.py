
import streamlit as st

from spotify_analysis.src.analysis.streaming_history_analyser import (
    StreamingHistoryAnalyser
)
from spotify_analysis.src.data.streaming_history import (
    StreamingHistory
)

spotify_download_link = "https://www.spotify.com/account/privacy/"

@st.cache_data
def read_stream_history(
    zip_path: st.runtime.uploaded_file_manager.UploadedFile,
) -> StreamingHistory:
    return StreamingHistory(zip_path).read_data().clean_data()

def get_data() -> StreamingHistory:
    # Upload the zip file
    zip_file_upload = st.file_uploader(
        "Upload your Spotify data",
        type="zip",
        accept_multiple_files=False,
        key="spotify_data_zip_upload",
        help=(
            "Upload your Spotify data ``.zip`` file. "
            "For more information on how to download your data, see "
            f"[here]({spotify_download_link})."
        ),
    )
    if zip_file_upload is None:
        st.stop()
    
    return read_stream_history(zip_file_upload)

def main():
    st.title("Spotify Data Analysis")
    with st.sidebar:
        stream_history = get_data()
    sha = StreamingHistoryAnalyser(stream_history)
    
    with st.sidebar:
        filter_by_year = st.checkbox(
            label="Filter by year",
            value=False,
            key="filter_by_year",
            help="Filter the data by year or show the entire dataset.",
        )
        if filter_by_year:
            year_selection = st.slider(
                label="Year range",
                min_value=2015,
                max_value=2024,
                value=2025,
            )
        else:
            year_selection = None
    
    (
        data_tab,
        daily_play_counts_tab,
        top_artists_tab,
        top_all_time_songs_tab,
        hyperfixation_songs_tab,
    ) = st.tabs([
        "Data",
        "Daily Play Counts",
        "Top artists",
        "Top all time songs",
        "Hyperfixation songs",
    ])
    
    year_plays_df = sha.get_cleaned_data(year=year_selection)
    
    if len(year_plays_df) == 0:
        st.warning(
            f"No data available for the selected year ({year_selection}). "
            "Please select another year or upload a different Spotify data file."
        )
        st.stop()
    
    with data_tab:
        st.write(year_plays_df)
    with daily_play_counts_tab:
        st.plotly_chart(
            sha.get_daily_mins_played_chart(
                year=year_selection,
            ),
        )
    with top_artists_tab:
        num_artists = st.slider("Number of artists", 1, 200, 20)
        st.altair_chart(
            sha.get_top_artists_bar_chart(
                year=year_selection,
                num_artists=num_artists,
            ),
            use_container_width=True,
        )
    with top_all_time_songs_tab:
        num_songs = st.slider(
            label="Number of songs",
            min_value=1,
            max_value=20,
            value=10,
            help="Number of songs to show in the chart.",
            key="num_top_songs",
        )
        st.altair_chart(
            sha.get_top_songs_cumulative_plays_chart(
                year=year_selection,
                num_songs=num_songs,
            ),
            use_container_width=True,
        )
    with hyperfixation_songs_tab:
        n_days: int = st.slider("Number of days", 1, 31, 7)
        hyperfixation_songs = sha.get_hyperfixation_songs(
            year=year_selection,
            n_days=n_days,
        )
        st.write(hyperfixation_songs)


if __name__ == "__main__":
    st.set_page_config(
        page_title="Spotify Data Analysis",
        page_icon=":bar_chart:",
        layout="wide",
        initial_sidebar_state="collapsed",
    )
    main()
