from flask import Flask, jsonify, redirect, request, url_for
from neo4j_connect import Neo4jConnection
from neo4j_queries import Neo4jQueries

app = Flask(__name__)
neo4j_conn = Neo4jConnection("bolt://localhost:7687", "neo4jj", "12345678")
#neo4j_conn = Neo4jConnection("bolt://3.215.134.19:7687", "neo4j", "sister-focuses-spills")
neo4j_conn.connect()


# APIs
"""
@app.route("/api/login", methods=["POST"])
def login():
    data = request.get_json()
    if "username" not in data:
        return jsonify({"error": "Users name is required"}), 400

    user_name = data["username"]
    if not neo4j_conn.execute_query(
        Neo4jQueries.get_user_by_name(), {"user_name": user_name}
    ):
        return jsonify({"error": "Invalid username"}), 401

    user = neo4j_conn.execute_query(
        Neo4jQueries.get_user_by_name(), {"user_name": user_name}
    )
    print("User Data:", user[0])
    return user[0]
"""
@app.route("/api/login", methods=["POST"])
def login():
    data = request.get_json()
    if "username" not in data:
        return jsonify({"error": "Users name is required"}), 400

    user_name = data["username"]
    print(f"Attempting login for user: {user_name}")

    user = neo4j_conn.execute_query(
        Neo4jQueries.get_user_by_name(), {"user_name": user_name}
    )
    if not user:
        print("User not found")
        return jsonify({"error": "Invalid username"}), 401

    print("User Data:", user[0])
    return user[0]
@app.route("/api/suggest_users", methods=["POST"])
def suggest_users():
    data = request.get_json()
    if "username" not in data:
        return jsonify({"error": "Users name is required"}), 400

    user_name = data["username"]
    if not neo4j_conn.execute_query(
        Neo4jQueries.get_user_by_name(), {"user_name": user_name}
    ):
        return jsonify({"error": "Invalid username"}), 401

    list_sugg_users = neo4j_conn.execute_query(
        Neo4jQueries.get_suggested_users(), {"user_name": user_name, "limit": 3}
    )
    return list_sugg_users


@app.route("/api/popular_movies", methods=["POST"])
def popular_movies():
    data = request.get_json()
    if "username" not in data:
        return jsonify({"error": "Users name is required"}), 400

    user_name = data["username"]
    if not neo4j_conn.execute_query(
        Neo4jQueries.get_user_by_name(), {"user_name": user_name}
    ):
        return jsonify({"error": "Invalid username"}), 401

    list_pop_movies = neo4j_conn.execute_query(
        Neo4jQueries.get_popular_movies(), {"limit": 6}
    )
    return list_pop_movies


@app.route("/api/rated_movies", methods=["POST"])
def rated_movies():
    data = request.get_json()
    if "username" not in data:
        return jsonify({"error": "Users name is required"}), 400

    user_name = data["username"]
    if not neo4j_conn.execute_query(
        Neo4jQueries.get_user_by_name(), {"user_name": user_name}
    ):
        return jsonify({"error": "Invalid username"}), 401

    list_rated_movies = neo4j_conn.execute_query(
        Neo4jQueries.get_movies_rated_by_user(), {"user_name": user_name, "limit": 6}
    )
    return list_rated_movies

@app.route('/api/suggest_movies_based_on_random', methods=['POST'])
def suggest_movies_based_on_actor():
    data = request.get_json()
    if 'username' not in data:
        return jsonify({'error': 'Users name is required'}), 400

    user_name = data['username']
    if not neo4j_conn.execute_query(Neo4jQueries.get_user_by_name(),{"user_name":user_name}):
        return jsonify({'error': 'Invalid username'}), 401

    list_suggested_movies_based_on_actor = neo4j_conn.execute_query(Neo4jQueries.get_suggested_movies_based_on_random(),{"user_name":user_name, "limit": 6})
    return list_suggested_movies_based_on_actor

@app.route('/api/suggest_movies_based_on_actor_in_movie', methods=['POST'])
def suggest_movies_based_on_actor_in_movie():
    data = request.get_json()
    if 'username' not in data:
        return jsonify({'error': 'Users name is required'}), 400

    user_name = data['username']
    if not neo4j_conn.execute_query(Neo4jQueries.get_user_by_name(),{"user_name":user_name}):
        return jsonify({'error': 'Invalid username'}), 401

    list_suggested_movies_based_on_actor_in_movie = neo4j_conn.execute_query(Neo4jQueries.get_suggested_movies_based_on_actor_in_movie(),{"user_name":user_name, "limit": 6})
    return list_suggested_movies_based_on_actor_in_movie

@app.route('/api/genres_based_on_popularity', methods=['POST'])
def get_genres_based_on_popularity():
    data = request.get_json()
    if 'username' not in data:
        return jsonify({'error': 'Users name is required'}), 400

    user_name = data['username']
    if not neo4j_conn.execute_query(Neo4jQueries.get_user_by_name(),{"user_name":user_name}):
        return jsonify({'error': 'Invalid username'}), 401

    list_genres_based_on_popularity = neo4j_conn.execute_query(Neo4jQueries.get_genres_based_on_popularity(),{"user_name":user_name, "limit": 6})
    return list_genres_based_on_popularity

@app.route('/api/suggest_latest_movies_based_on_actor', methods=['POST'])
def suggest_latest_movies_based_on_actor():
    data = request.get_json()
    if 'username' not in data:
        return jsonify({'error': 'Users name is required'}), 400

    user_name = data['username']
    if not neo4j_conn.execute_query(Neo4jQueries.get_user_by_name(),{"user_name":user_name}):
        return jsonify({'error': 'Invalid username'}), 401

    list = neo4j_conn.execute_query(Neo4jQueries.suggest_latest_movies_based_on_actor(),{"user_name":user_name, "limit": 1})
    return list

@app.route('/api/suggest_latest_movies_based_on_director', methods=['POST'])
def suggest_latest_movies_based_on_director():
    data = request.get_json()
    if 'username' not in data:
        return jsonify({'error': 'Users name is required'}), 400

    user_name = data['username']
    if not neo4j_conn.execute_query(Neo4jQueries.get_user_by_name(),{"user_name":user_name}):
        return jsonify({'error': 'Invalid username'}), 401

    list = neo4j_conn.execute_query(Neo4jQueries.suggest_latest_movies_based_on_director(),{"user_name":user_name, "limit": 1})
    return list

@app.route('/api/suggest_latest_movies_based_on_genre', methods=['POST'])
def suggest_latest_movies_based_on_genre():
    data = request.get_json()
    if 'username' not in data:
        return jsonify({'error': 'Users name is required'}), 400

    user_name = data['username']
    if not neo4j_conn.execute_query(Neo4jQueries.get_user_by_name(),{"user_name":user_name}):
        return jsonify({'error': 'Invalid username'}), 401

    list = neo4j_conn.execute_query(Neo4jQueries.suggest_latest_movies_based_on_genre(),{"user_name":user_name, "limit": 1})
    return list

@app.route("/api/get_recommended_movie_by_favorite_genre", methods=["POST"])
def get_recommended_movie_by_favorite_genre():
    data = request.get_json()
    if "username" not in data:
        return jsonify({"error": "Users name is required"}), 400

    user_name = data["username"]
    if not neo4j_conn.execute_query(
        Neo4jQueries.get_user_by_name(), {"user_name": user_name}
    ):
        return jsonify({"error": "Invalid username"}), 401

    recommended_movie = neo4j_conn.execute_query(
        Neo4jQueries.get_recommended_movie_by_favorite_genre(), {"user_name": user_name, "limit": 6}
    )
    return recommended_movie

@app.route('/api/home/<int:user_id>')
def home(user_id):
    return jsonify({"message": f"Welcome to the home page, user {user_id}!"})


if __name__ == "__main__":
    try:
        neo4j_conn.connect()
        print("Connected to Neo4j")
    except Exception as e:
        print(f"Failed to connect to Neo4j: {e}")
    app.run(debug=True)
