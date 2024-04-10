import os
import dotenv
from neo4j import GraphDatabase


load_status = dotenv.load_dotenv('Neo4j-Credentials.txt')

if not load_status:
    raise RuntimeError('Environment variables not loaded')

URI = os.getenv('NEO4J_URI')
AUTH = (os.getenv('NEO4J_USERNAME'), os.getenv('NEO4J_PASSWORD'))


def create_node(tx, labels, properties):
    if labels is None:
        raise ValueError('Labels not provided')
    
    if properties is None:
        raise ValueError('Properties not provided')
    
    query = f"""
        CREATE (n:{':'.join(labels)} $properties)
        RETURN n
        """
    result = tx.run(query, properties=properties)
    print(f'SUCCES: Node with label: {labels} and properties: {properties} created\n') 
    return result.consume()


try:
    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        driver.verify_connectivity()
        print('SUCCES: Connection to Neo4j instance established\n')
        with driver.session() as session:
            # Prueba de creacion de nodo
            result = create_node(session, ['Person'], {'name': 'Alice', 'age': 25})

except Exception as e:
    print(f'ERROR: {e}')

finally:
    driver.close()