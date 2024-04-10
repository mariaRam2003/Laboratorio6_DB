import os
import dotenv
from datetime import datetime
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

    users = [record["u"] for record in result]
    if not users:
        return f"No user found with {'ID' if user_id else 'name'}: {user_id if user_id else user_name}"
    return f"User(s) found: {', '.join([str(user['name']) for user in users])}"


def find_movie(tx, movie_id=None, movie_title=None):
    if movie_id:
        query = "MATCH (m:Movie) WHERE m.movieID = $movie_id RETURN m"
        result = tx.run(query, movie_id=movie_id)
    elif movie_title:
        query = "MATCH (m:Movie) WHERE m.title = $movie_title RETURN m"
        result = tx.run(query, movie_title=movie_title)
    else:
        raise ValueError('Movie ID or Movie Title is required')

    movies = [record["m"] for record in result]
    if not movies:
        return f"No movie found with {'ID' if movie_id else 'title'}: {movie_id if movie_id else movie_title}"
    return f"Movie(s) found: {', '.join([str(movie['title']) for movie in movies])}"


def find_user_rating(tx, user_id, movie_id):
    query = """
    MATCH (u:User)-[r:RATED]->(m:Movie)
    WHERE u.userID = $user_id AND m.movieID = $movie_id
    RETURN u, r, m
    """
    result = tx.run(query, user_id=user_id, movie_id=movie_id)
    ratings = [{"user": record["u"], "rating": record["r"], "movie": record["m"]} for record in result]
    if not ratings:
        return f"No rating found for user ID {user_id} on movie ID {movie_id}"
    return f"Rating found: User {ratings[0]['user']['name']} rated movie {ratings[0]['movie']['title']} with {ratings[0]['rating']['rating']} stars"


try:
    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        driver.verify_connectivity()
        print('SUCCES: Connection to Neo4j instance established')
        with driver.session() as session:
            # Creacion de nodo persona actor director (PAD)
            node_PAD = Neo4jNode(labels=['Person', 'Actor', 'Director'], 
                                 properties={'name': 'Tom Hanks', 
                                             'tmdbld': 31,
                                             'born': datetime.strptime('01-01-1956', '%d-%m-%Y'),
                                             'died': 'Not Dead',
                                             'bornIn': 'Concord, California, USA',
                                             'url:': 'tomhanks.com',
                                             'imdbld': 158,
                                             'bio':'Tom Hanks bio Here',
                                             'poster': 'tomhanks.jpg'})
            create_node(session, node_PAD)

            # Creacion de nodo persona actor (PA)
            node_PA = Neo4jNode(labels=['Person', 'Actor'], 
                                properties={'name': 'Tom Cruise', 
                                            'tmdbld': 500,
                                            'born': datetime.strptime('01-01-1962', '%d-%m-%Y'),
                                            'died': 'Not Dead',
                                            'bornIn': 'Syracuse, New York, USA',
                                            'url:': 'tomcruise.com',
                                            'imdbld': 500,
                                            'bio':'Tom Cruise bio Here',
                                            'poster': 'tomcruise.jpg'})
            create_node(session, node_PA)

            # Creacion de nodo persona director (PD)
            node_PD = Neo4jNode(labels=['Person', 'Director'], 
                                properties={'name': 'Steven Spielberg', 
                                            'tmdbld': 488,
                                            'born': datetime.strptime('01-01-1946', '%d-%m-%Y'),
                                            'died': 'Not Dead',
                                            'bornIn': 'Cincinnati, Ohio, USA',
                                            'url:': 'stevenspielberg.com',
                                            'imdbld': 488,
                                            'bio':'Steven Spielberg bio Here',
                                            'poster': 'stevenspielberg.jpg'})
            create_node(session, node_PD)

            # Creacion de nodo usuario (User)
            node_user = Neo4jNode(labels=['User'], 
                                  properties={'name': 'User1', 
                                              'userID': 1})
            create_node(session, node_user)

            # Creacion de nodo pelicula (Movie)
            node_movie = Neo4jNode(labels=['Movie'], 
                                   properties={'title': 'Forrest Gump', 
                                               'tmdbld': 13,
                                               'released': datetime.strptime('01-01-1994', '%d-%m-%Y'),
                                               'imdbRating': 8,
                                               'movieID': 1,
                                               'year': 1994,
                                               'imdbld': 13,
                                               'runtime': 142,
                                               'countries': ['USA', 'India'],
                                               'imdbVotes': 1800,
                                               'url': 'forrestgump.com',
                                               'revenue': 1000000,
                                               'plot': 'Forrest Gump plot Here',
                                               'poster': 'forrestgump.jpg',
                                               'budget': 900000,
                                               'languages': ['English', 'Hindi']})
            create_node(session, node_movie)

            # Creacion de nodo genero (Genre)
            node_genre = Neo4jNode(labels=['Genre'], 
                                   properties={'name': 'Drama'})
            create_node(session, node_genre)

            # Creacion de relaciones entre nodos
            # Relacion PAD con pelicula
            relationship1 = Neo4jRelationship(node1=node_PAD, 
                                              node2=node_movie, 
                                              relationship='ACTED_IN', 
                                              properties={'role': 'Forrest Gump'})
            create_relationship(session, relationship1)
            relationship2 = Neo4jRelationship(node1=node_PAD, 
                                              node2=node_movie, 
                                              relationship='DIRECTED', 
                                              properties={'role': 'Visual Director'})
            create_relationship(session, relationship2)

            # Relaciones PA con pelicula
            relationship3 = Neo4jRelationship(node1=node_PA, 
                                              node2=node_movie, 
                                              relationship='ACTED_IN', 
                                              properties={'role': 'Forrest Gump'})
            create_relationship(session, relationship3)

            # Relaciones PD con pelicula
            relationship4 = Neo4jRelationship(node1=node_PD, 
                                              node2=node_movie, 
                                              relationship='DIRECTED', 
                                              properties={'role': 'General Director'})
            create_relationship(session, relationship4)

            # Relacion USER con pelicula
            relationship5 = Neo4jRelationship(node1=node_user, 
                                              node2=node_movie, 
                                              relationship='RATED', 
                                              properties={'rating': 5,
                                                          'timestamp': 105})
            create_relationship(session, relationship5)

            # Relacion GENRE con pelicula
            relationship6 = Neo4jRelationship(node1=node_genre, 
                                              node2=node_movie, 
                                              relationship='GENRE', 
                                              properties={})
            create_relationship(session, relationship6)

except Exception as e:
    print(f'ERROR: {e}')

finally:
    if driver:
        driver.close()