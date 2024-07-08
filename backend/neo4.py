import requests
from neo4j import GraphDatabase

# Configura tu API Key de TMDb
API_KEY = 'b7f0f51a97bec30353439facd850caf1'
BASE_URL = 'https://api.themoviedb.org/3'

# Configura la conexión a Neo4j 
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "123456789"

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

def fetch_movies(page):
    response = requests.get(f'{BASE_URL}/movie/popular', params={'api_key': API_KEY, 'page': page})
    return response.json().get('results', [])

def load_movie_to_neo4j(movie):
    with driver.session() as session:
        session.run(
            """
            MERGE (m:Movie {id: $id})
            ON CREATE SET m.title = $title, m.overview = $overview, m.release_date = $release_date
            """,
            id=movie['id'], title=movie['title'], overview=movie.get('overview', ''), release_date=movie.get('release_date', '')
        )

def main():
    for page in range(1, 51):  # Esto obtendrá 1000 películas (20 por página, 50 páginas)
        movies = fetch_movies(page)
        for movie in movies:
            load_movie_to_neo4j(movie)

if __name__ == "__main__":
    main()
