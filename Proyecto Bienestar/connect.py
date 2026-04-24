#!/usr/bin/env python3

import logging
import os
import pydgraph
import requests
from cassandra.cluster import Cluster

#Logger para el historial
log = logging.getLogger()
log.setLevel('INFO')
handler = logging.FileHandler('app.log')
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)


#___________________________________________________________________________________
#CASSANDRA
CLUSTER_IPS        = os.getenv('CASSANDRA_CLUSTER_IPS', '127.0.0.1')
KEYSPACE           = os.getenv('CASSANDRA_KEYSPACE', 'bienestar')
REPLICATION_FACTOR = os.getenv('CASSANDRA_REPLICATION_FACTOR', '1')

#___________________________________________________________________________________
#MONGO
MONGO_URL = os.getenv('MONGO_URL', 'http://localhost:8000')

#___________________________________________________________________________________
#DGRAPH
DGRAPH_URI = os.getenv('DGRAPH_URI', 'localhost:9080')


#Funcion para la conexion a Cassandra
def connect_cassandra():
    log.info("Connecting to Cluster") 
    cluster = Cluster(CLUSTER_IPS.split(','))
    session = cluster.connect()
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': {REPLICATION_FACTOR}}}
    """)
    session.set_keyspace(KEYSPACE)
    log.info(f"Connected to keyspace {KEYSPACE}")
    return cluster, session

#funcion para cerrar la conexion a Cassandra
def close_cassandra(cluster):
    cluster.shutdown()
    log.info("Cassandra connection closed")


#funcion para la conexion a MongoDB
def connect_mongo():
    log.info("Conectando a MongoDB via Falcon...")
    try:
        requests.get(MONGO_URL)
        log.info("Servidor Falcon disponible en %s", MONGO_URL)
    except requests.exceptions.ConnectionError:
        log.error("No se puede conectar a Falcon en %s — ¿está corriendo?", MONGO_URL)
    return MONGO_URL

#funcion para cerrar la conexion a MongoDB
def close_mongo():
    log.info("MongoDB (Falcon) desconectado.")


#funciones para la conexion a Dgraph
def connect_dgraph():
    log.info("Connecting to Dgraph")
    client_stub = pydgraph.DgraphClientStub(DGRAPH_URI)
    client = pydgraph.DgraphClient(client_stub)
    log.info(f"Connected to Dgraph at {DGRAPH_URI}")
    return client_stub, client

def close_dgraph(client_stub):
    client_stub.close()


def init_connections():
    cassandra_cluster, cassandra_session = connect_cassandra()
    mongo_url                            = connect_mongo()
    dgraph_stub, dgraph_client           = connect_dgraph()
    return {
        "cassandra_cluster": cassandra_cluster,
        "cassandra_session": cassandra_session,
        "mongo_url":         mongo_url,
        "dgraph_stub":       dgraph_stub,
        "dgraph_client":     dgraph_client,
    }
"""
Uso de IA
En esta funcion, se juntan las conexiones en un diccionario para que desde main
se pueda acceder a cada una de las conexiones de manera sencilla, sin necesidad de llamar a cada funcion de conexion por separado.
"""




def close_connections(conns):
    close_cassandra(conns["cassandra_cluster"])
    close_mongo()
    close_dgraph(conns["dgraph_stub"])