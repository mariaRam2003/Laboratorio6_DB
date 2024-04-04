from neo4j import GraphDatabase

class GraphDatabaseService:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def create_node(self, label, attributes):
        with self.driver.session() as session:
            session.execute_write(self._create_node, label, attributes)

    @staticmethod
    def _create_node(tx, label, attributes):
        # Atributos del nodo
        attr_str = ', '.join(f"{key}: ${key}" for key in attributes.keys())
        
        # Consulta de la creacion
        query = f"CREATE (n:{label} {{{attr_str}}})"
        
        # Ejecucion de la consulta
        tx.run(query, **attributes)


def read_auth_file(file_path):
        uri = user = password = ""
        with open(file_path, 'r') as file:
            lines = file.readlines()
            for line in lines:
                if "NEO4J_URI=" in line:
                    uri = line.split('=')[1].strip()
                elif "NEO4J_USERNAME=" in line:
                    user = line.split('=')[1].strip()
                elif "NEO4J_PASSWORD=" in line:
                    password = line.split('=')[1].strip()
        return uri, user, password

def main():
    # Obtener credenciales de autenticacion
    auth_file_path = "auth.txt"
    uri, user, password = read_auth_file(auth_file_path)

    graph_db = GraphDatabaseService(uri, user, password)

    try:
        # Crear un nodo de usuario de prueba
        user_attributes = {"name": "Alice", "userid": "alice123"}
        graph_db.create_node("User", user_attributes)
        print("Nodo de usuario creado exitosamente")
    finally:
        graph_db.close()

if __name__ == "__main__":
    main()
