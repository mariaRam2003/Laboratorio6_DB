import os
import dotenv
import random
from neo4j import GraphDatabase


load_status = dotenv.load_dotenv('Neo4j-Credentials.txt')

if not load_status:
    raise RuntimeError('Environment variables not loaded')

URI = os.getenv('NEO4J_URI')
AUTH = (os.getenv('NEO4J_USERNAME'), os.getenv('NEO4J_PASSWORD'))

class Neo4jNode:
    def __init__(self, labels, properties):
        if labels is None:
            raise ValueError('Labels not provided')
        if properties is None:
            raise ValueError('Properties not provided')
        
        self.labels = labels
        self.properties = properties

class Neo4jRelationship:
    def __init__(self, node1, node2, relationship, properties):
        if node1 is None or node2 is None:
            raise ValueError('Nodes not provided')
        if relationship is None:
            raise ValueError('Relationship type not provided')

        self.node1 = node1
        self.node2 = node2
        self.type = relationship
        self.properties = properties
    

def create_node(tx, node: Neo4jNode):
    query = f"""
        CREATE (n:{':'.join(node.labels)} $properties)
        RETURN n
        """
    result = tx.run(query, properties=node.properties)
    print(f'SUCCES: Node with labels: {node.labels} and properties: {node.properties} created') 

    return result.consume()

def create_relationship(tx, relationship: Neo4jRelationship):
    # Extract the first property's key and value for both nodes
    node1_key, node1_value = next(iter(relationship.node1.properties.items()))
    node2_key, node2_value = next(iter(relationship.node2.properties.items()))

    query = (
        f"MATCH (a:{':'.join(relationship.node1.labels)}), "
        f"(b:{':'.join(relationship.node2.labels)}) "
        f"WHERE a.{node1_key} = $node1_value AND b.{node2_key} = $node2_value "
        f"CREATE (a)-[r:{relationship.type} $rel_properties]->(b) "
        f"RETURN type(r)"
    )

    # Execute the query, passing the first property's value for each node
    result = tx.run(query, 
                    node1_value=node1_value, 
                    node2_value=node2_value, 
                    rel_properties=relationship.properties or {})
    print(f'SUCCESS: Relationship {relationship.type} created between {node1_value} and {node2_value}')

    return result.consume()

def find_user(tx, user_id=None, user_name=None):
    if user_id:
        query = "MATCH (u:User) WHERE u.userID = $user_id RETURN u"
        result = tx.run(query, user_id=user_id)
    elif user_name:
        query = "MATCH (u:User) WHERE u.name = $user_name RETURN u"
        result = tx.run(query, user_name=user_name)
    else:
        raise ValueError('User ID or User Name is required')

    return [record["u"] for record in result]

def find_movie(tx, movie_id=None, movie_title=None):
    if movie_id:
        query = "MATCH (m:Movie) WHERE m.movieID = $movie_id RETURN m"
        result = tx.run(query, movie_id=movie_id)
    elif movie_title:
        query = "MATCH (m:Movie) WHERE m.title = $movie_title RETURN m"
        result = tx.run(query, movie_title=movie_title)
    else:
        raise ValueError('Movie ID or Movie Title is required')

    return [record["m"] for record in result]

def find_user_rating(tx, user_id, movie_id):
    query = """
    MATCH (u:User)-[r:RATED]->(m:Movie)
    WHERE u.userID = $user_id AND m.movieID = $movie_id
    RETURN u, r, m
    """
    result = tx.run(query, user_id=user_id, movie_id=movie_id)
    return [{"user": record["u"], "rating": record["r"], "movie": record["m"]} for record in result]

try:
    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        driver.verify_connectivity()
        print('SUCCES: Connection to Neo4j instance established')
        with driver.session() as session:
            # Creacion de nodos de usuario
            user1 = Neo4jNode(labels=['User'], properties={'name': 'user1', 'userID': 1})
            create_node(session, user1)

            # Creacion de nodos de peliculas
            movie1 = Neo4jNode(labels=['Movie'], properties={'title': 'movie1', 'movieID': 1, 'year': random.randint(2000, 2021), 'plot': 'plot of the movie 1'})
            create_node(session, movie1)
            
            # Creacion de relaciones Rated usuario -> pelicula
            relationship1 = Neo4jRelationship(node1=user1, node2=movie1, relationship='RATED', properties={'rating': random.randint(1, 5), 'timestamp': random.randint(1, 100)})
            create_relationship(session, relationship1)

            # Busquedas
            user_found = find_user(session, user_id=1)
            print(f'\nUser Found: \n{user_found}')

            movie_found = find_movie(session, movie_id=1)
            print(f'\nMovie Found: \n{movie_found}')

            found_relation = find_user_rating(session, user_id=1, movie_id=1)
            print(f'\nRelationship Found: {found_relation}')

except Exception as e:
    print(f'ERROR: {e}')

finally:
    if driver:
        driver.close()