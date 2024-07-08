# neo4j_queries.py

class Neo4jQueries:
    #SIMPLE GET NODE TYPE QUERIES 
    @staticmethod
    def get_user_by_name():
        return (
            "MATCH (u:User {name: $user_name}) "
            "RETURN { name: u.name, userId: u.userId } AS user"
        )
    @staticmethod
    def get_movie_by_title():
        return (
            "MATCH (m:Movie {title: $movie_title}) "
            "RETURN { title: m.title, imdbId: m.imdbId, plot: m.plot } AS movie"
        )
    @staticmethod
    def get_movies_rated_by_user():
        return (
            "MATCH (me:User {name: $user_name})-[my:RATED]->(m:Movie) "
            "RETURN m.title AS movie, my.rating AS rating "
            "ORDER BY rating DESC "
            "LIMIT $limit "
        )
    #SIMPLE GET RELATIONSHIP TYPE QUERIES
    @staticmethod
    def get_R_rated():
        return (
            "MATCH (m:Movie {title: $movie_title})-[r:RATED]-(u:User {name: $user_name}) "
            "RETURN { movie: m.title, user: u.name, rating: r.rating} AS rating"
        )
    #COMPLEX GET NODE TYPE QUERIES
    @staticmethod
    def get_suggested_users():
        return (
            "MATCH (me:User {name: $user_name})-[my:RATED]->(m:Movie) "
            "MATCH (other:User)-[their:RATED]->(m) "
            "WHERE me <> other "
            "AND abs(my.rating - their.rating) < 0.5 "
            "WITH other, COUNT(m) AS n, COLLECT(m.title) as Movies_in_common "
            "RETURN other.name AS Friend_suggestion,n , Movies_in_common "
            "ORDER BY n DESC "
            "LIMIT $limit "
        )
    @staticmethod
    def get_popular_movies():
        return (
            "MATCH (m:Movie)<-[r:RATED]-() "
            "MATCH (m:Movie)-[IN_GENRE]->(g:Genre) "
            "RETURN COUNT(r) AS N_of_reviews, m.title as Movie, m.year as Year ,COLLECT(DISTINCT g.name) as Genre "
            "ORDER BY N_of_reviews DESC "
            "LIMIT $limit"
        ) 
    @staticmethod  
    def get_suggested_movies_based_on_random():
        return (
            "MATCH (u:User {name: $user_name}) "
            "MATCH (m:Movie) "
            "WHERE NOT (u)-[:RATED]->(m) "
            "WITH m.title AS Movie, m.year AS Year "
            "RETURN Movie, Year "
            "ORDER BY RAND() " 
            "LIMIT $limit;"
        )
    @staticmethod
    def get_suggested_movies_based_on_actor_in_movie():
        return (
            "MATCH (u:User {name: $user_name})-[r:RATED]->(m:Movie) "
            "MATCH (m)-[:ACTED_IN]-(a:Actor) "
            "MATCH (a)-[:ACTED_IN]-(mm:Movie) "
            "MATCH (mm)-[rr:RATED]-() "
            "MATCH (mm)-[:DIRECTED]-(d:Director) "
            "WHERE m <> mm AND r.rating >= 4.0 "
            "WITH u, m.title AS MyRatedMovie, a.name AS Actor, mm.title AS RecommendedMovie, mm.plot AS Plot, mm.year AS Year, mm.poster AS Poster, d.name AS Director  "
            "RETURN MyRatedMovie, Actor, RecommendedMovie, Plot, Year, Poster, Director "
            "ORDER BY RAND() "
            "LIMIT $limit "
        )
    @staticmethod
    def get_genres_based_on_popularity():
        return (
            "MATCH (m:Movie)-[:IN_GENRE]->(g:Genre) "
            "MATCH (u:User)-[r:RATED]->(m) "
            "RETURN g.name AS Genre, COUNT(r) AS ReviewCount "
            "ORDER BY ReviewCount DESC "
            "LIMIT $limit"
        )
    @staticmethod
    def suggest_latest_movies_based_on_actor():
        return (
            "MATCH (u:User {name: $user_name})-[r:RATED]-(m:Movie) "
            "MATCH (m)-[:ACTED_IN]-(a:Actor) "
            "MATCH (a)-[:ACTED_IN]-(mm:Movie) "
            "WHERE m <> mm AND mm.year IS NOT NULL AND r.rating <= 4.0 " 
            "RETURN mm.title AS Movie, mm.year AS Year, a.name AS Actor "
            "ORDER BY mm.year DESC, RAND() "
            "LIMIT $limit "
        )
    @staticmethod
    def suggest_latest_movies_based_on_director():
        return (
            "MATCH (u:User {name: $user_name})-[r:RATED]-(m:Movie) "
            "MATCH (m)-[:DIRECTED]-(d:Director) "
            "MATCH (d)-[:DIRECTED]-(mm:Movie) "
            "WHERE m <> mm AND mm.year IS NOT NULL AND r.rating <= 4.0 " 
            "RETURN mm.title AS Movie, mm.year AS Year, d.name AS Director "
            "ORDER BY mm.year DESC, RAND() "
            "LIMIT $limit "
        )
    @staticmethod
    def suggest_latest_movies_based_on_genre():
        return (
            "MATCH (u:User {name: $user_name})-[r:RATED]-(m:Movie) "
            "MATCH (m)-[:IN_GENRE]-(g:Genre) "
            "MATCH (g)-[:IN_GENRE]-(mm:Movie) "
            "WHERE m <> mm AND mm.year IS NOT NULL AND r.rating <= 4.0 "
            "RETURN DISTINCT mm.title AS Movie, mm.year AS Year, g.name AS Genre "
            "ORDER BY mm.year DESC, RAND() "
            "LIMIT $limit"
        )
    @staticmethod
    def get_recommended_movie_by_favorite_genre():
        return (
            "MATCH (u:User {name: $user_name}) - [r:RATED]-(m:Movie) - [: IN_GENRE]-(genre:Genre) "
            "WITH u, genre, COUNT(m) AS moviesRated "
            "ORDER BY moviesRated DESC "
            "LIMIT 1 "

            "MATCH (u)-[r2: RATED]-(m2:Movie)-[: IN_GENRE]-(genre) "
            "WITH u, genre, COLLECT(m2) AS ratedMovies "

            "MATCH(unratedMovie: Movie)-[: IN_GENRE]-(genre) "
            "WHERE NOT(u) - [: RATED]-(unratedMovie) "
            
            "MATCH (director:Director)-[:DIRECTED]-(unratedMovie)"
            
            "RETURN unratedMovie.title AS title, unratedMovie.poster AS poster, unratedMovie.plot AS plot, "
            "unratedMovie.year AS year, genre.name AS FavoriteGenre, director.name AS director "
            "ORDER BY RAND() "
            "LIMIT $limit; "
            )
    #CREATE NODE TYPE QUERIES
    @staticmethod
    def create_node_user():
        return ()
    #CREATE RELATIONSHIP TYPE QUERIES
    def create_relation_rating():
        return ()