#!/usr/bin/env python3

import logging
import os
import pydgraph
from cassandra.cluster import Cluster
from pymongo import MongoClient

#Logger para el historial
log = logging.getLogger()
log.setLevel('INFO')
handler = logging.FileHandler('app.log')
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)


#___________________________________________________________________________________
#CASSANDRA

# Read env vars related to Cassandra App
CLUSTER_IPS = os.getenv('CASSANDRA_CLUSTER_IPS', '127.0.0.1')
KEYSPACE = os.getenv('CASSANDRA_KEYSPACE', 'bienestar')
REPLICATION_FACTOR = os.getenv('CASSANDRA_REPLICATION_FACTOR', '1')

#___________________________________________________________________________________
#MONGO

MONGO_URI = os.getenv('MONGO_URI', 'mongodb://localhost:27017/')
MONGO_DB  = os.getenv('MONGO_DB',  'bienestar')


#___________________________________________________________________________________
#DGRAPH
DGRAPH_URI = os.getenv('DGRAPH_URI', 'localhost:9080')


#Funcion para la conexion a Cassandra
def connect_cassandra():
    log.info("Connecting to Cluster") 
    cluster = Cluster(CLUSTER_IPS.split(','))
    session = cluster.connect()
    #las 3 anteriores líneas se encargan de conectar con el cluster de Cassandra utilizando
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
    log.info("Connecting to MongoDB")
    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client[MONGO_DB]
    """
    AQUI VAN LOS INDICES
    """
    log.info(f"Connected to MongoDB database {MONGO_DB}")
    return mongo_client, db

#funcion para cerrar la conexion a MongoDB
def close_mongo(mongo_client):
    mongo_client.close()
    log.info("MongoDB connection closed")



#funciones para la conexion a Dgraph
def connect_dgraph():
    log.info("Connecting to Dgraph")
    client_stub = pydgraph.DgraphClientStub(DGRAPH_URI)
    client = pydgraph.DgraphClient(client_stub)
    log.info(f"Connected to Dgraph at {DGRAPH_URI}")
    return client_stub, client


def close_dgraph(client_stub):
    """Cierra la conexión con Dgraph."""
    client_stub.close()


def init_connections():
    cassandra_cluster, cassandra_session = connect_cassandra()
    mongo_client, mongo_db = connect_mongo()
    dgraph_client_stub, dgraph_client = connect_dgraph()
    return {
        "cassandra_cluster": cassandra_cluster,  
        "cassandra_session": cassandra_session,  
        "mongo_client":      mongo_client, 
        "mongo_db":          mongo_db,     
        "dgraph_stub":       dgraph_client_stub,   
        "dgraph_client":     dgraph_client, 
    }

def close_connections(connections):
    close_cassandra(connections["cassandra_cluster"])
    close_mongo(connections["mongo_client"])
    close_dgraph(connections["dgraph_stub"])