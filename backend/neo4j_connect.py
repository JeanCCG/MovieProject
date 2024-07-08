#neo4j_connect.py

from neo4j import GraphDatabase, basic_auth
from neo4j.exceptions import Neo4jError
import json
#from neo4j_queries import Neo4jQueries

class Neo4jConnection:

    def __init__(self, uri, username, password):
        #self.id_global = 1000
        self._uri = uri
        self._username = username
        self._password = password
        self._driver = None

    def close(self):
        if self._driver is not None:
            self._driver.close()

    def connect(self):
        self._driver = GraphDatabase.driver(self._uri, auth=basic_auth(self._username, self._password))
        
    def execute_query_and_save_to_file(self, output_file_path, query, parameters=None):
        try:
            with self._driver.session() as session:
                result = session.run(query,parameters)
                records = [dict(record) for record in result]
            with open(output_file_path, "w") as output_file:
                json.dump(records, output_file, indent=2)
            print(f"Result saved to {output_file_path}")
        except Exception as e:
            print(f"Error: {e}")
    
    def execute_query(self, query, parameters=None):
        try:
            with self._driver.session() as session:
                result = session.run(query, parameters)
                records = [dict(record) for record in result]
            return records
        except Neo4jError as e:
            print(f"Neo4jError: {e}")
            return None

        

def main():
    """
    # SANDBOX
    neo4j_conn = Neo4jConnection("bolt://44.197.239.196:7687", "neo4j", "recruit-presence-captain")
    # LOCAL SANDBOX
    neo4j_conn = Neo4jConnection("neo4j://localhost:7687", "neo4j", "4jneo")

    neo4j_conn.connect()

    test_query = neo4j_conn.execute_query(Neo4jQueries.get_user_by_name(),{"user_name":"Margaret Allen"})
    if not test_query:
        print("No")
    else:
        print(json.dumps(test_query, indent=2))

    test_query = neo4j_conn.execute_query(Neo4jQueries.get_movie_by_title(),{"movie_title":"Toy Story"})
    if not test_query:
        print("No")
    else:
        print(json.dumps(test_query, indent=2))

    test_query = neo4j_conn.execute_query(Neo4jQueries.get_suggested_users(),{"user_name":"Margaret Allen","limit": 3})
    if not test_query:
        print("No")
    else:
        print(json.dumps(test_query, indent=2))

    neo4j_conn.close()
    """

if __name__ == "__main__":
    main()