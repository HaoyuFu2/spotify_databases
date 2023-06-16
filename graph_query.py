def parse_track_id(row):
    return row.split("'")[1]

def louvain_cluster(session):
    # Cluster the artist nodes into communities using Louvain algorithm from GDS
    # Add community_id as a property to every artist node

    # Drop the GDS graph if exists
    drop_query = """
        CALL gds.graph.drop('artists', false)
    """
    session.run(drop_query)

    # Create a new GDS graph
    project_query = """
        CALL gds.graph.project(
        'artists',
        'Artist',
        'COLLABORATE'
    )
    """
    session.run(project_query)

    # Perform Louvain Clustering on the artists graph
    # Update the communityId property
    louvain_query = """
        CALL gds.louvain.stream('artists')
        YIELD nodeId, communityId
        SET gds.util.asNode(nodeId).community_id = communityId
    """
    session.run(louvain_query)

def recommend_track(r, session, track_id, limit=10):

    recommend_query = """
        MATCH (t1:Track {track_id: '"""+ track_id +"""'})
        MATCH (t1)-[:COMPOSED_BY]->(a:Artist)
        MATCH (t2)-[:COMPOSED_BY]->(a)
        RETURN t2.track_id AS track_id, t2.track_name AS track_name,
        gds.similarity.pearson(
            [t1.danceability, t1.energy, t1.key, t1.loudness, t1.liveness, t1.tempo],
            [t2.danceability, t2.energy, t2.key, t2.loudness, t2.liveness, t2.tempo]
        ) AS pearsonSimilarity
        ORDER BY pearsonSimilarity DESCENDING
    """ + f"LIMIT {limit}"


    # Redis    
    result = r.lrange(recommend_query, 0, -1)
    
    if result:
        return result
    else:
        results = session.run(recommend_query).data()
        for line in results:
            r.rpush(recommend_query, str(line))
        return r.lrange(recommend_query, 0, -1)