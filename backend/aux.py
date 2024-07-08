from neo4j_connect import Neo4jConnection

neo4j_conn = Neo4jConnection(
    "bolt://3.215.134.19:7687", "neo4j", "sister-focuses-spills"
)
neo4j_conn.connect()

query = (
    "MATCH (u1:User) "
    "WITH u1, rand() AS r "
    "ORDER BY r "
    "LIMIT 1 "
    "MATCH (u2:User) "
    "WHERE u2 <> u1 "
    "WITH u1, u2 "
    "ORDER BY rand() "
    "LIMIT 1 "
    "CREATE (u1)-[:Friend]->(u2); "
)

for i in range(600):
    print(i)
    neo4j_conn.execute_query(query)
    neo4j_conn.execute_query(query)
    neo4j_conn.execute_query(query)
