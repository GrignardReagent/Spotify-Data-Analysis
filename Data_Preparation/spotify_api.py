

import requests
import pandas as pd
from tqdm import tqdm
import sys
import os

token = sys.argv[1]
path = sys.argv[2]

headers = {
    'Authorization': 'Bearer  ' + token}


# Load the original Spotify dataset
# path_to_dataset = "Spotify_Dataset_V3.csv"
spotify_dataframe = pd.read_csv(path, sep = ";")


# Obtain all the unique track ids
unique_track_ids = spotify_dataframe["id"].unique()


# Construct a dataframe for artists in each unique track
artist_dataframe = pd.DataFrame(columns= ["id", "Artist ID", "# of Artist", "Followers"])

track_ids = []
artist_ids = []
artist_number = []
artist_followers = []


# Obtain the artist IDs for all unique tracks in batches of 50
for i in tqdm(range(0,len(unique_track_ids) , 50)):
    
    # Make a Spotify API request to obtain information about 50 tracks
    track_ids_req = ",".join(unique_track_ids[i: i + 50])
    track_url = f"https://api.spotify.com/v1/tracks?ids={track_ids_req}"
    track_response = requests.get(track_url, headers = headers).json()
    
    # Iterate over the response tracks, add the relevant information to the respective lists
    for track in track_response["tracks"]:
      artists = track["artists"]
      for number, artist in enumerate(artists):
          
        track_ids.append(track["id"])
        artist_ids.append(artist["id"])
        artist_number.append("Artist " + str(number + 1))

# Add the track id, artist id and artist name information to the output dataframe
artist_dataframe["id"] = track_ids
artist_dataframe["Artist ID"] = artist_ids
artist_dataframe["# of Artist"] = artist_number

print(artist_dataframe.iloc[1,:])

# Obtain the information about the artists 
for i in tqdm(range(0,len(artist_ids), 50)):
    
  # Make a Spotify API request to obtain information about 50 artists
  query_string = ",".join(artist_ids[i: i+50])
  track_url = f"https://api.spotify.com/v1/artists?ids={query_string}"
  track_response = requests.get(track_url, headers = headers).json()

  # Extract artist follower information
  for artist in track_response["artists"]:
      
      artist_followers = artist["followers"]["total"]
      artist_id = artist["id"]
      
      mask = artist_dataframe["Artist ID"] == artist_id
      artist_dataframe.loc[mask,"Followers"] = artist_followers
      

# Merge the original dataframe with the artist dataframe,
# such that all song, artist pairs get added a follower entry
updated_df = spotify_dataframe.merge(artist_dataframe, on=["id", "# of Artist"], how="inner")
updated_df.drop("Artist ID", axis=1)

# Save the updated dataframe to a file
updated_df.to_csv(os.path.join("..", "Data","spotify_updated.csv"), index = None)


