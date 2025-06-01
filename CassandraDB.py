from cassandra.cluster import Cluster

def connect_to_cassandra(keyspace):
    cluster = Cluster(['127.0.0.1']) 
    session = cluster.connect(keyspace)
    return session