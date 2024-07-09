from tmdbv3api import TMDb, Movie, Person, Genre
from neo4j import GraphDatabase
import random

# source venv/bin/activate
# Configura tu API Key de TMDb
tmdb = TMDb()
tmdb.api_key = 'b7f0f51a97bec30353439facd850caf1'

# Instancia de las clases de Movie, Person y Genre
movie_api = Movie()
person_api = Person()
genre_api = Genre()

# Configura tu conexión a Neo4j
uri = "bolt://localhost:7687"
username = "neo4jj"
password = "12345678"



# Obtener datos de una película por su ID
def get_movie_data(movie_id):
    movie = movie_api.details(movie_id)
    credits = movie_api.credits(movie_id)
    
    if movie:
        return {
            "title": movie.title,
            "imdb_id": movie.imdb_id,
            "plot": movie.overview,
            "year": movie.release_date.split('-')[0] if movie.release_date else None,
            "poster": movie.poster_path,
            "genres": [genre['name'] for genre in movie.genres],
            "director": next((person['name'] for person in credits['crew'] if person['job'] == 'Director'), None),
            "actors": [cast['name'] for cast in credits['cast']]
        }
    return None

# Obtener datos de una persona por su nombre
def get_person_data(name):
    search_results = person_api.search(name)
    if search_results:
        person = search_results[0]  # Obtener el primer resultado
        return {
            "name": person.name,
            "biography": getattr(person, 'biography', None),
            "birth_date": getattr(person, 'birthday', None),
            "profile_path": getattr(person, 'profile_path', None)
        }
    return None

# Crear nodo de película en Neo4j
def create_movie_node(tx, movie_data):
    query = """
    MERGE (m:Movie {title: $title})
    SET m.imdbId = $imdb_id, m.plot = $plot, m.year = $year, m.poster = $poster
    """
    tx.run(query, movie_data)

def create_user_node(tx, user_name):
    query = """
    CREATE (:User {name: $name})
    """
    tx.run(query, name=user_name)

def create_rated_relationship(tx, user_name, movie_title, rating):
    query = """
    MATCH (u:User {name: $user_name})
    MATCH (m:Movie {title: $movie_title})
    MERGE (u)-[:RATED {rating: $rating}]->(m)
    """
    tx.run(query, user_name=user_name, movie_title=movie_title, rating=rating)

# Crear nodo de persona (actor o director) en Neo4j
def create_person_node(tx, person_data):
    query = """
    MERGE (p:Person {name: $name})
    SET p.biography = $biography, p.birth_date = $birth_date, p.profile_path = $profile_path
    """
    tx.run(query, person_data)

# Crear nodo de género en Neo4j
def create_genre_node(tx, genre_name):
    query = """
    MERGE (g:Genre {name: $name})
    """
    tx.run(query, name=genre_name)

# Crear relación ACTED_IN entre actor y película
def create_acted_in_relationship(tx, movie_title, person_name):
    query = """
    MATCH (m:Movie {title: $movie_title})
    MATCH (p:Person {name: $person_name})
    MERGE (p)-[:ACTED_IN]->(m)
    """
    tx.run(query, movie_title=movie_title, person_name=person_name)

# Crear relación DIRECTED entre director y película
def create_directed_relationship(tx, movie_title, person_name):
    query = """
    MATCH (m:Movie {title: $movie_title})
    MATCH (p:Person {name: $person_name})
    MERGE (p)-[:DIRECTED]->(m)
    """
    tx.run(query, movie_title=movie_title, person_name=person_name)

# Crear relación IN_GENRE entre película y género
def create_in_genre_relationship(tx, movie_title, genre_name):
    query = """
    MATCH (m:Movie {title: $movie_title})
    MATCH (g:Genre {name: $genre_name})
    MERGE (m)-[:IN_GENRE]->(g)
    """
    tx.run(query, movie_title=movie_title, genre_name=genre_name)

# Función principal
def main():
    # Crear nodo de usuario
    user_name = "UCSP"
    popular_movies = movie_api.popular()
    movie_ids = [movie.id for movie in popular_movies][:15]  # Obtener los primeros 15 IDs de películas
    with driver.session() as session:
        session.execute_write(create_user_node, user_name)

        for movie_id in movie_ids:
            movie_data = get_movie_data(movie_id)
            rating = random.uniform(1.0, 10.0)
            # Crear nodo de película
            session.execute_write(create_movie_node, movie_data)
            # Crear nodo de director y relacionar con la película
            if movie_data["director"]:
                director_data = get_person_data(movie_data["director"])
                if director_data:
                    session.execute_write(create_person_node, director_data)
                    session.execute_write(create_directed_relationship, movie_data["title"], director_data["name"])
            # Crear nodos de actores y relacionar con la película
            for actor in movie_data["actors"]:
                actor_data = get_person_data(actor)
                if actor_data:
                    session.execute_write(create_person_node, actor_data)
                    session.execute_write(create_acted_in_relationship, movie_data["title"], actor_data["name"])
            # Crear nodos de géneros y relacionar con la película
            for genre in movie_data["genres"]:
                session.execute_write(create_genre_node, genre)
                session.execute_write(create_in_genre_relationship, movie_data["title"], genre)
            if random.choice([True, False]):  # Relacionar aleatoriamente
                session.execute_write(create_rated_relationship, user_name, movie_data["title"], rating)

if __name__ == "__main__":
    # Establecer conexión a Neo4j
    driver = GraphDatabase.driver(uri, auth=(username, password))

    # Ejecutar el flujo principal
    main()

    # Cerrar la conexión a Neo4j al finalizar
    driver.close()