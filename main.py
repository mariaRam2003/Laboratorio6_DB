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


try:
    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        driver.verify_connectivity()
        print('SUCCES: Connection to Neo4j instance established')
        with driver.session() as session:
            # Creacion de 5 nodos de usuario
            user1 = Neo4jNode(labels=['User'], properties={'name': 'user1', 'userID': 1})
            create_node(session, user1)
            user2 = Neo4jNode(labels=['User'], properties={'name': 'user2', 'userID': 2})
            create_node(session, user2)
            user3 = Neo4jNode(labels=['User'], properties={'name': 'user3', 'userID': 3})
            create_node(session, user3)
            user4 = Neo4jNode(labels=['User'], properties={'name': 'user4', 'userID': 4})
            create_node(session, user4)
            user5 = Neo4jNode(labels=['User'], properties={'name': 'user5', 'userID': 5})
            create_node(session, user5)

            # Creacion de nodos de peliculas
            movie1 = Neo4jNode(labels=['Movie'], properties={'title': 'movie1', 'movieID': 1, 'year': random.randint(2000, 2021), 'plot': 'plot of the movie 1'})
            create_node(session, movie1)
            movie2 = Neo4jNode(labels=['Movie'], properties={'title': 'movie2', 'movieID': 2, 'year': random.randint(2000, 2021), 'plot': 'plot of the movie 2'})
            create_node(session, movie2)
            movie3 = Neo4jNode(labels=['Movie'], properties={'title': 'movie3', 'movieID': 3, 'year': random.randint(2000, 2021), 'plot': 'plot of the movie 3'})
            create_node(session, movie3)
            movie4 = Neo4jNode(labels=['Movie'], properties={'title': 'movie4', 'movieID': 4, 'year': random.randint(2000, 2021), 'plot': 'plot of the movie 4'})
            create_node(session, movie4)
            movie5 = Neo4jNode(labels=['Movie'], properties={'title': 'movie5', 'movieID': 5, 'year': random.randint(2000, 2021), 'plot': 'plot of the movie 5'})
            create_node(session, movie5)
            movie6 = Neo4jNode(labels=['Movie'], properties={'title': 'movie6', 'movieID': 6, 'year': random.randint(2000, 2021), 'plot': 'plot of the movie 6'})
            create_node(session, movie6)
            movie7 = Neo4jNode(labels=['Movie'], properties={'title': 'movie7', 'movieID': 7, 'year': random.randint(2000, 2021), 'plot': 'plot of the movie 7'})
            create_node(session, movie7)
            movie8 = Neo4jNode(labels=['Movie'], properties={'title': 'movie8', 'movieID': 8, 'year': random.randint(2000, 2021), 'plot': 'plot of the movie 8'})
            create_node(session, movie8)
            movie9 = Neo4jNode(labels=['Movie'], properties={'title': 'movie9', 'movieID': 9, 'year': random.randint(2000, 2021), 'plot': 'plot of the movie 9'})
            create_node(session, movie9)
            movie10 = Neo4jNode(labels=['Movie'], properties={'title': 'movie10', 'movieID': 10, 'year': random.randint(2000, 2021), 'plot': 'plot of the movie 10'})
            create_node(session, movie10)
            
            # Creacion de 2 relaciones por usuario
            relationship1 = Neo4jRelationship(node1=user1, node2=movie1, relationship='RATED', properties={'rating': random.randint(1, 5), 'timestamp': random.randint(1, 100)})
            create_relationship(session, relationship1)
            relationship2 = Neo4jRelationship(node1=user1, node2=movie2, relationship='RATED', properties={'rating': random.randint(1, 5), 'timestamp': random.randint(1, 100)})
            create_relationship(session, relationship2)
            relationship3 = Neo4jRelationship(node1=user2, node2=movie3, relationship='RATED', properties={'rating': random.randint(1, 5), 'timestamp': random.randint(1, 100)})
            create_relationship(session, relationship3)
            relationship4 = Neo4jRelationship(node1=user2, node2=movie4, relationship='RATED', properties={'rating': random.randint(1, 5), 'timestamp': random.randint(1, 100)})
            create_relationship(session, relationship4)
            relationship5 = Neo4jRelationship(node1=user3, node2=movie5, relationship='RATED', properties={'rating': random.randint(1, 5), 'timestamp': random.randint(1, 100)})
            create_relationship(session, relationship5)
            relationship6 = Neo4jRelationship(node1=user3, node2=movie6, relationship='RATED', properties={'rating': random.randint(1, 5), 'timestamp': random.randint(1, 100)})
            create_relationship(session, relationship6)
            relationship7 = Neo4jRelationship(node1=user4, node2=movie7, relationship='RATED', properties={'rating': random.randint(1, 5), 'timestamp': random.randint(1, 100)})
            create_relationship(session, relationship7)
            relationship8 = Neo4jRelationship(node1=user4, node2=movie8, relationship='RATED', properties={'rating': random.randint(1, 5), 'timestamp': random.randint(1, 100)})
            create_relationship(session, relationship8)
            relationship9 = Neo4jRelationship(node1=user5, node2=movie9, relationship='RATED', properties={'rating': random.randint(1, 5), 'timestamp': random.randint(1, 100)})
            create_relationship(session, relationship9)
            relationship10 = Neo4jRelationship(node1=user5, node2=movie10, relationship='RATED', properties={'rating': random.randint(1, 5), 'timestamp': random.randint(1, 100)})
            create_relationship(session, relationship10)

except Exception as e:
    print(f'ERROR: {e}')

finally:
    if driver:
        driver.close()