import os
import dotenv
from neo4j import GraphDatabase


load_status = dotenv.load_dotenv('Neo4j-Credentials.txt')

if not load_status:
    raise RuntimeError('Environment variables not loaded')

URI = os.getenv('NEO4J_URI')
AUTH = (os.getenv('NEO4J_USERNAME'), os.getenv('NEO4J_PASSWORD'))

try:
    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        driver.verify_connectivity()
        print('Successfully connected to Neo4j Instance')

except Exception as e:
    print(f'Error: {e}')