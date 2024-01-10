from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster,  ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import RoundRobinPolicy, DowngradingConsistencyRetryPolicy, WhiteListRoundRobinPolicy

all_nodes = ['192.168.48.185', '192.168.48.184','192.168.48.192', '192.168.48.191','192.168.48.193']

profile = ExecutionProfile(
    load_balancing_policy=WhiteListRoundRobinPolicy(all_nodes),
    retry_policy=DowngradingConsistencyRetryPolicy(),
    consistency_level=ConsistencyLevel.QUORUM,
    # serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL,
    # request_timeout=15,
    # row_factory=tuple_factory
)
cluster = Cluster(execution_profiles={EXEC_PROFILE_DEFAULT: profile})

def get_conn_details():
    # Create a connection to the Cassandra cluster
    policy = RoundRobinPolicy()
    # Connect to the Cassandra cluster
    cluster = Cluster(
        contact_points = all_nodes,
        execution_profiles={EXEC_PROFILE_DEFAULT: profile},
        port=9042
    )
    # session = cluster.connect('teambnewarchitecture')
    session = cluster.connect()
    keyspace = "teambgoing"

    keyspace_query = f"""
        CREATE KEYSPACE IF NOT EXISTS {keyspace}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 3}};
    """

    session.execute(keyspace_query)

    session.set_keyspace(keyspace)

    return cluster,session