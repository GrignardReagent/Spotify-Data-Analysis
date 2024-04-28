
import pandas as pd
import numpy as np
import os

# Import the Spotify dataset containing Top 200 playlists
path_to_dataset = os.path.join("..", "Data", "spotify_updated.csv")
spotify_dataframe = pd.read_csv(path_to_dataset, sep = ",")

# Obtain a list of all dates between the first and last day in a dataset
spotify_dataframe["Date"] = pd.to_datetime(spotify_dataframe["Date"], format = "%d/%m/%Y")
first_date = spotify_dataframe.iloc[-1,:]["Date"]
last_date = spotify_dataframe.iloc[0,:]["Date"]

days_within_range = np.arange(np.datetime64(first_date), np.datetime64(last_date) +1, np.timedelta64(1, "D"))
days_in_df = spotify_dataframe["Date"].drop_duplicates().values


# Obtain a list of dates, along with the missing ranks 
days_ranks_missing = []
for day in days_in_df:
    
    songs_on_day = spotify_dataframe[spotify_dataframe["Date"] == day]
    ranks_on_day = songs_on_day.drop_duplicates("Rank")
    
    if ranks_on_day.shape[0] != 200:
        
        missing_rank = set(np.arange(1,201)) - set(ranks_on_day["Rank"].values)
        days_ranks_missing.append((day, missing_rank))


# Define a +/- 50 rank range in which to look for the missing song
range = 50
rows_to_append = []

# Iterate over all the days when some ranks are missing
for current_date, missing_ranks in days_ranks_missing:
    
    # Obtain the date for the previous and next day of the missing rank day
    previous_date = current_date - np.timedelta64(1, "D")
    next_date = current_date + np.timedelta64(1, "D")
    
    # Obtain all the unique songs in a given day
    songs_today = spotify_dataframe[spotify_dataframe["Date"] == current_date].drop_duplicates(["Title", "Artists"])
    
    missing_rank_differences = {}
    
    # Iterate over the missing ranks on the given day
    for rank in list(missing_ranks):

        # For each rank find the list of possible songs that were present on the previous and 
        # the following day in a +-50 rank range from the missing rank that weren't present
        # on the given day.
        songs_previous_day = spotify_dataframe[spotify_dataframe["Date"] == previous_date].drop_duplicates(["Title", "Artists"]).sort_values("Rank").iloc[rank-range:rank+range,:]

        songs_next_day = spotify_dataframe[spotify_dataframe["Date"] == previous_date].drop_duplicates(["Title", "Artists"]).sort_values("Rank").iloc[rank-range:rank+range,:]
        
        missing_songs = (set(songs_previous_day["Title"].values) & set(songs_next_day["Title"].values)) - set(songs_today["Title"].values)
    
        # If no missing songs found, leave the missing rank as it is    
        if len(missing_songs) == 0:
            continue
            
        differences = {}
        
        # For all the possible missing songs calculate the sum of their absolute differences to the missing rank
        for missing_song in missing_songs:
            rank_previous_day = songs_previous_day[songs_previous_day["Title"] == missing_song]["Rank"].values[0]
            rank_next_day = songs_previous_day[songs_next_day["Title"] == missing_song]["Rank"].values[0]
            
            differences[missing_song] = abs(rank - rank_previous_day) + abs(rank - rank_next_day)
            
        # For every rank store the possible songs with their respective differences
        missing_rank_differences[rank] = differences
        
    # Sort the song differences in an ascending order for every missing rank
    sorted_ranks = {rank: sorted(differences.items(), key=lambda x: x[1]) for rank, differences in missing_rank_differences.items()}

    selected_songs = set()  
    rank_new_songs = {}  
    
    song_selected = False

    # Iterate over missing ranks
    for rank, songs in sorted_ranks.items():
        # Iterate over possible songs
        for song, _ in songs:
            # If song hasn't been selected for any of the missing rank, select it 
            # for the current missing rank
            if song not in selected_songs:
                selected_songs.add(song)
                rank_new_songs[rank] = song
                break


    songs_previous_day = spotify_dataframe[spotify_dataframe["Date"] == previous_date].drop_duplicates(["Title", "Artists"])
    
    # Construct the new rows for the missing ranks
    for rank, song in rank_new_songs.items():
        
        # Copy the row for the song from the previous day and change the 
        # 'Rank','Points (Total)' and 'Date' columns/
        song_row = songs_previous_day[songs_previous_day["Title"] == song].copy()
        song_row.loc[:,"Rank"] = rank
        song_row.loc[:,"Points (Total)"] = 200 - rank + 1
        song_row.loc[:,"Date"] = current_date
        
        # Add the new row to the dataframe
        spotify_dataframe = pd.concat([spotify_dataframe,song_row], axis = 0)
        rows_to_append.append(song_row)

# Change the 'Date' column back to the original format
spotify_dataframe['Date'] = spotify_dataframe['Date'].apply(lambda x: pd.to_datetime(x).strftime('%d/%m/%Y'))
# Save the updated dataframe to the file
spotify_dataframe.to_csv(os.path.join("..", "Data", "spotify_updated_2.csv"), index = False)

