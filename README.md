# Spotify Databases

## Team: Yixuan Zhang, Eddie Ho, Haoyu Fu

### Project Description
The objective of this project is to develop a database application for storing and analyzing datasets from Spotify. Its primary aim is to enable fast and reliable access to data, with the capability to retrieve track and artist information based on various filtering criteria such as track name, tonality, estimated time signatures, key signatures, and more. Additionally, this application will integrate a recommendation system, which can suggest similar tracks to users based on their query results. In this project, we use Redis for caching to swiftly fetch query results; choose Cassandra and PostgreSQL as our database systems due to their suitability for our varied use cases; utilize Neo4j as a graph database to better represent and query data relationships; and employ Pearson Similarity as the algorithm for our recommendation system, to provide more precise suggestions.

### Files

- **Slides_Spotify_Datasets.pdf**: Final report presentation, offering a comprehensive overview and key insights from the project.
- **graph_query.py**: Implements Neo4j for graph database management and develops functions for querying within the graph database, enhancing data retrieval and analysis capabilities.
- **tabular_query.py**: Incorporates both Cassandra and PostgreSQL for database management. It includes functions for executing queries in different database environments, demonstrating flexibility in data handling.
- **data_cleaning.py**: Python script dedicated to cleaning and preparing the dataset for analysis, ensuring data quality and consistency.
- **data**: A folder containing the datasets used in this project, serving as the foundational data source for analysis and visualization.

### Instructions
1. **Starting the Application**: Begin by running codes from **demo.ipynb**.
2. **Data Preparation**: Unzip the files inside **data/raw**.
3. **Data Cleaning**: After completing the Data Cleaning section in **demo.ipynb**, copy all files from **data/cleaned/network/** to the Neo4j directory at **/import/cleaned**.
   
### Technology Stack
- **Redis**
- **Cassandra**
- **PostgreSQL**
- **Neo4j**

### Key Features
- Rapid and reliable data access.
- Advanced filtering capabilities for searching tracks and artists.
- Integrated recommendation system using Pearson Similarity for personalized suggestions.

### Purpose
The project is designed to enhance the user experience on Spotify by providing quick access to detailed track and artist information, and by recommending tracks based on user preferences and query results.
