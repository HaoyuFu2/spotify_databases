import redis
import psycopg2
import pandas as pd
from tqdm import tqdm
from psycopg2 import extras
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement


##############################################
#            PostgreSQL Functions            #
##############################################
def search_artist(r, cur, **kwargs):
    """
    :param r: Redis connection object
    :param cur: PostgreSQL connection cursor
    :param id: id of the artist (String)
    :param followers: number of followers the artist has on Spotify (Int)
    :param genres: genres associated with the artist (List as a string)
    :param name: name of the artist (String)
    :param popularity: popularity score of the artist (Int)
    
    :param projection: information to retrieve as per user's request (List)
    :param limit: number of rows to be returned (Int)

    @Return: Creates a PostgreSQL query based on the provided information. If 
    the query has been executed before, the function will retrieve the corres
    ponding cached result from Redis and return it. If it is a new query, the 
    function will return the rows that should be freshly retrieved from Postg
    reSQL, and store them in Redis.

    @Function Explanation: This function takes multiple variables to create a
    PostgreSQL query, which includes the projection step ('id', 'followers', 
    'genres', 'name', 'popularity'), the selection step ('projection'), and t
    he limit step ('limit'). It returns the rows retrieved based on these con
    ditions. The function first attempts to retrieve the query from Redis. If
    it is not found in Redis, it executes a new query, stores the query in th
    e Redis cache, and returns the retrieved results to the user. However, if
    the query has been previously cached in Redis, it will retrieve the resul
    ts from Redis and return them to the user.
    """
    
    parameters = {
        "id": kwargs.get("id"),
        "followers": kwargs.get("followers"),
        "genres": kwargs.get("genres"),
        "name": kwargs.get("name"),
        "popularity": kwargs.get("popularity"),
        "projection": kwargs.get(
            "projection", ["id", "followers", "genres", "name", "popularity"]
        ),
        "limit": kwargs.get("limit"),
    }

    # Projection Formatting
    projection = parameters["projection"]
    projection_formatting = " , ".join(projection)

    # Selection Formatting
    selection = {
        k: v
        for k, v in parameters.items()
        if v is not None and k != "projection" and k != "limit"
    }
    selection_formatting = (
        "WHERE "
        + " AND ".join(
            f"{k} = '{v}'" if isinstance(v, str) else f"{k} = {v}"
            for k, v in selection.items()
        )
        if len(selection) != 0
        else ""
    )

    # Limit Formatting
    limit_formatting = (
        f"LIMIT {parameters['limit']}" if parameters["limit"] is not None else ""
    )

    query = f"""
        SELECT
        {projection_formatting}
        FROM artist_df
        {selection_formatting}
        {limit_formatting}
    """
    print("Query:", query)
    
    # Redis
    result = r.get(query)

    if result:
        return result
    else:
        cur.execute(query)
        result = str(cur.fetchall())
        r.set(query, result)
        return result

###############################################
#             Cassandra Functions             #
###############################################


#######################
# popularity_by_month #
#######################

def insertion_popularity_by_month(session, df):
    batch = BatchStatement()
    insert_query = \
        session.prepare('INSERT INTO month_popularity (year, month, popularity, id) VALUES (?, ?, ?, ?);'
                        )

    for (index, row) in df.iterrows():
        batch.add(insert_query, (pd.to_datetime(row['release_date'
                  ]).year, pd.to_datetime(row['release_date']).month,
                  row['popularity'], row['id']))

        if (index + 1) % 500 == 0:
            session.execute(batch)
            batch = BatchStatement()

    if len(batch) > 0:
        session.execute(batch)

def popularity_by_month(r, session, year, month):
    """
    :param year: The year from the date specified by the user (Int)
    :param month: The month from the date specified by the user (Int)

    @Return: This function will return the query result in Cassandra. Based on
    the year and month provided by the user, this function will return the num
    ber of tracks of various popularity levels for that particular month.
    
    @Function Explanation: This function is used to query the number of tracks 
    of various popularity levels for a specific month, according to the year a
    nd month provided by the user.
    """
    query = f"""
        SELECT year, month, popularity, count(id) AS count
        FROM month_popularity
        WHERE year = {year} AND month = {month}
        GROUP BY popularity;
    """
    
    result = r.lrange(query, 0, -1)

    if result:
        # Redis
        return result
    else:
        new = session.execute(query)
        for line in new:
            r.rpush(query, str(line))
        
        result = r.lrange(query, 0, -1)
        return result
    
#############################
# track_by_music_attributes #
#############################

def insertion_music_attributes(session, df):
    batch = BatchStatement()
    insert_query = \
        session.prepare('INSERT INTO music_attributes (key, mode, time_signature, id, name) VALUES (?, ?, ?, ?, ?);')

    for (index, row) in df.iterrows():
        batch.add(insert_query, (row['key'], row['mode'],
                  row['time_signature'], row['id'], row['name']))

        if (index + 1) % 300 == 0:
            session.execute(batch)
            batch = BatchStatement()

    if len(batch) > 0:
        session.execute(batch)

def track_by_music_attributes(r, session, key, mode, time_signature, limit = None):
    """
    :param key: The key the track is in. Integers map to pitches using standard Pitch 
    Class notation. E.g. 0 = C, 1 = C♯/D♭, 2 = D, and so on. If no key was detected, 
    the value is -1. (Int) 
    :param mode: Mode indicates the modality [major 1 or minor 0] of a track, the type 
    of scale from which its melodic content is derived. (Int) 
    :param time_signature: An estimated time signature. The time signature [meter] is 
    a notational convention to specify how many beats are in each bar. (Int)  
    :param limit: number of rows to be returned (Int)
    
    @Return: 
    @Function Explanation:
    """
    
    if limit is not None:
        query = f"""
            SELECT 
                id, 
                name
            FROM music_attributes
            WHERE key = {key} AND mode = {mode} AND time_signature = {time_signature}
            LIMIT {limit}
        """
    else:
        query = f"""
            SELECT 
                id, 
                name
            FROM music_attributes
            WHERE key = {key} AND mode = {mode} AND time_signature = {time_signature}
        """
        
    result = r.lrange(query, 0, -1)
    
    if result:
        # Redis
        return result
    else:
        new = session.execute(query)
        for line in new:
            r.rpush(query, str(line))
        
        result = r.lrange(query, 0, -1)
        return result