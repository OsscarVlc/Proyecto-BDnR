"""
Carga los datos a todas las bases de datos.
"""

from connect import init_connections, close_connections
from Dgraph import model as model_dgraph
from Cassandra import model as model_cassandra


def main():
    print("Iniciando conexiones...")
    conexion = init_connections()
    print("¡Conexiones establecidas!\n")

    dgraph_client = conexion["dgraph_client"]
    cassandra_session = conexion["cassandra_session"]

    print("=== Dgraph ===")
    model_dgraph.load_data(dgraph_client)

    print("=== Cassandra ===")
    model_cassandra.load_data(cassandra_session)


    print("\nCerrando conexiones...")
    close_connections(conexion)
    print("¡Datos cargados correctamente!")


if __name__ == '__main__':
    main()