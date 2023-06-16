import pandas as pd
import csv
"""
Data Sources:
    --: Redis, Cassandra, PostgreSQL
    @artist_df: https://www.kaggle.com/datasets/andrewmvd/spotify-playlists?select=spotify_dataset.csv
    @track_df: https://www.kaggle.com/datasets/andrewmvd/spotify-playlists?select=spotify_dataset.csv
    
    --: Neo4j
    @nodes: https://www.kaggle.com/datasets/jfreyberg/spotify-artist-feature-collaboration-network?select=nodes.csv
    @edges: https://www.kaggle.com/datasets/jfreyberg/spotify-artist-feature-collaboration-network?select=nodes.csv  
"""

# Helper function for dataframe processing
def process_df(df, path):
    df = df.dropna().copy()

    for col in ['genres', 'name', 'artists']:
        if col in df.columns:
            df.loc[:, col] = df[col].str.replace('""', '"'
                    ).str.replace('"', "'").str.replace(', ', ',')

    if 'followers' in df.columns:
        df.loc[:, 'followers'] = df['followers'].astype('int64')

    if 'release_date' in df.columns:
        df.loc[:, 'release_date'] = pd.to_datetime(df['release_date'
                ]).dt.strftime('%Y-%m-%d')

    df.to_csv(path, index=False, escapechar='\\',
              quoting=csv.QUOTE_NONE, sep='|')

    return df

def clean_data_tabular():
    # Tabular Dataset
    artist_df = pd.read_csv('data/raw/tabular/artists.csv')
    track_df = pd.read_csv('data/raw/tabular/tracks.csv')

    artist_df = process_df(artist_df,
                        'data/cleaned/tabular/artists_clean.csv')
    track_df = process_df(track_df, 'data/cleaned/tabular/tracks_clean.csv')

    return artist_df, track_df

def clean_data_graph(track_df):
    # Network Datasets
    nodes = pd.read_csv('data/raw/network/nodes.csv')
    edges = pd.read_csv('data/raw/network/edges.csv')

    # Using a subset of nodes & edges for demo
    artist_nodes = nodes[nodes.popularity > 70] # Select the artists with popularity > 70
    artist_nodes.to_csv('data/cleaned/network/artist_nodes.csv', index=False)
    artist_ids = set(artist_nodes.spotify_id)
    collaborations = edges[edges.id_0.isin(artist_ids) & edges.id_1.isin(artist_ids)]
    collaborations.to_csv('data/cleaned/network/collaborations.csv', index=False)

    # Select the tracks composed by above artists and perform data cleaning
    track_df['demo'] = track_df['id_artists']\
        .apply(lambda lst: any([x in artist_ids for x in lst[1:-1].replace("'", '').split(', ')]))
    track_nodes = track_df[track_df.demo == True][['id','name','popularity','duration_ms','artists','id_artists']]
    track_nodes['id_artists'] = track_nodes['id_artists']\
        .astype('str').apply(lambda x: x[1:-1].replace("'","").replace(", ",","))
    track_nodes.to_csv('data/cleaned/network/track_nodes.csv', index=False)
    
    return artist_nodes, track_nodes, collaborations