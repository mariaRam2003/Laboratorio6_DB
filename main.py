import os
import dotenv
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
            # Prueba de creacion de nodo
            person_node = Neo4jNode(['Person'], {'name': 'string', 'userID': 'string'})
            movie_node = Neo4jNode(['Movie'], {'title': 'string', 'movieID': 'string', 'year': 'integer', 'plot':'string'})
            create_node(session, person_node)
            create_node(session, movie_node)
            rated_relationship = Neo4jRelationship(person_node, movie_node, 'RATED', {'rating': 'integer'})
            create_relationship(session, rated_relationship)

except Exception as e:
    print(f'ERROR: {e}')

finally:
    if driver:
        driver.close()