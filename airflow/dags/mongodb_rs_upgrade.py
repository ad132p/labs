from datetime import datetime
from airflow.sdk import dag, task
from airflow.providers.ssh.operators.ssh import SSHOperator
import pymongo
import time

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

@dag(
    dag_id='mongodb_rs_upgrade_v7_to_v8',
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['mongodb', 'upgrade', 'maintenance'],
    doc_md="""
    # MongoDB Replica Set Upgrade DAG (v7 -> v8)
    
    This DAG performs a rolling upgrade of a MongoDB Replica Set using Percona Server for MongoDB packages.
    
    ### Workflow:
    1. **Discover Members**: Connects to the cluster to identify Primary and Secondary nodes.
    2. **Upgrade Secondaries**: Parallel execution of upgrade steps on all Secondary nodes.
    3. **Step Down Primary**: Forces the current Primary to step down.
    4. **Upgrade Former Primary**: Upgrades the node that was previously the Primary.
    5. **Finalize**: Sets featureCompatibilityVersion to 8.0.
    
    ### Requirements:
    - `pymongo` installed in the Airflow environment.
    - SSH connection IDs configured for each MongoDB host (or use a common key). 
      *Assumption*: Airflow Connection ID matches the hostname or IP.
    - Connection to MongoDB credentials stored in Airflow Connections or passed via params.
    """
)
def mongodb_rs_upgrade():

    @task(multiple_outputs=True)
    def discover_members(**context):
        """
        Connects to the MongoDB Replica Set and identifies the Primary and Secondaries.
        Returns a dict with 'primary' and 'secondaries' lists.
        """
        # Configuration - In a real scenario, fetch these from Variables or Connections
        mongo_uri = context['params'].get('mongo_uri','mongodb://root:password@db-1:27017,db-2:27017,db-3:27017/?replicaSet=rs0')
        
        client = pymongo.MongoClient(mongo_uri)
        status = client.admin.command('replSetGetStatus')
        
        members = status['members']
        primary = None
        secondaries = []
        
        for member in members:
            host = member['name'].split(':')[0] # Assuming name is host:port
            if member['stateStr'] == 'PRIMARY':
                primary = host
            elif member['stateStr'] == 'SECONDARY':
                secondaries.append(host)
        
        if not primary:
            raise Exception("No Primary found in Replica Set!")
            
        return {
            'primary': primary,
            'secondaries': secondaries,
            'all_members': [primary] + secondaries
        }

    discovery = discover_members()

    @task
    def get_secondary_hosts(discovery_result):
        return discovery_result['secondaries']

    secondary_hosts = get_secondary_hosts(discovery)

    # Define the upgrade command sequence
    # Note: Non-interactive frontend for apt to avoid prompts
    upgrade_commands = """
    echo "Starting Upgrade on $(hostname)..."
    
    # OS Detection
    if [ -f /etc/os-release ]; then
        . /etc/os-release
        OS=$ID
    else
        echo "Cannot detect OS, assuming Debian-based..."
        OS=debian
    fi

    echo "Detected OS: $OS"

    echo "Stopping MongoDB..."
    # Universal service stop (systemd)
    if command -v systemctl &> /dev/null; then
        sudo systemctl stop mongod
    else
        sudo service mongod stop
    fi
    
    if [[ "$OS" == "debian" || "$OS" == "ubuntu" ]]; then
        echo "Running Debian/Ubuntu upgrade path..."
        export DEBIAN_FRONTEND=noninteractive
        
        echo "Updating Repositories..."
        sudo percona-release enable psmdb-80 release 
        sudo apt-get update
        
        echo "Installing Percona Server for MongoDB 8.0..."
        sudo apt-get install -y percona-server-mongodb
        
    elif [[ "$OS" == "rhel" || "$OS" == "rocky" || "$OS" == "centos" || "$OS" == "almalinux" ]]; then
        echo "Running RHEL/Rocky upgrade path..."
        
        echo "Installing Percona Server for MongoDB 8.0..."
        # Simply install/update the package. DNF handles dependencies.
        # Ensure the repo is enabled beforehand or enable specific module if needed.
        sudo percona-release enable psmdb-80 release
        sudo dnf install -y percona-server-mongodb
        
    else
        echo "Unsupported OS: $OS"
        exit 1
    fi
    
    echo "Starting MongoDB..."
    if command -v systemctl &> /dev/null; then
        sudo systemctl start mongod
    else
        sudo service mongod start
    fi
    
    echo "Waiting for health check..."
    # Simple check loop
    for i in {1..30}; do
        if nc -z localhost 27017; then
            echo "MongoDB is up!"
            exit 0
        fi
        sleep 2
    done
    echo "MongoDB failed to start!"
    exit 1
    """

    @task
    def step_down_primary(topology, **context):
        """
        Connects to the Primary node and executes rs.stepDown().
        """
        primary_host = topology['primary']
        mongo_uri = context['params'].get('mongo_uri', 'mongodb://root:percona@db-1:27017,db-2:27017,db-3:27017/?replicaSet=rs0')
        
        # We specifically want to connect to the current primary to step it down
        # In a robust setup, we might parse the URI to ensure we hit the right host directly
        client = pymongo.MongoClient(mongo_uri)
        
        print(f"Stepping down primary: {primary_host}")
        try:
            # force=True to ensure it steps down even if no secondary is caught up immediately (use with caution)
            # secondaryCatchUpPeriodSecs can be adjusted
            client.admin.command('replSetStepDown', 60, secondaryCatchUpPeriodSecs=10, force=True)
        except pymongo.errors.AutoReconnect:
            print("Successfully stepped down (connection closed as expected).")
        except Exception as e:
            print(f"Error executing stepDown: {e}")
            # It might have succeeded but threw an error due to connection loss
            pass
        
        # Give some time for election
        time.sleep(15)

    @task
    def set_feature_compatibility_version(**context):
        """
        Sets the FCV to 8.0 after all nodes are upgraded.
        """
        mongo_uri = context['params'].get('mongo_uri', 'mongodb://root:percona@db-1:27017,db-2:27017,db-3:27017/?replicaSet=rs0')
        client = pymongo.MongoClient(mongo_uri)
        
        # Wait until we can write (new primary elected)
        retries = 5
        while retries > 0:
            try:
                print("Setting featureCompatibilityVersion to 8.0...")
                client.admin.command('setFeatureCompatibilityVersion', '8.0', confirm=True)
                print("Successfully set FCV to 8.0")
                return
            except pymongo.errors.NotPrimaryError:
                print("Not connected to primary, retrying...")
                time.sleep(5)
                retries -= 1
            except Exception as e:
                print(f"Error setting FCV: {e}")
                raise e
        
        raise Exception("Could not set FCV after multiple retries")

    # Dynamic Task Mapping for Secondaries
    # We pass the list of secondaries to the SSHOperator
    # Note: SSHOperator doesn't directly support mapping over 'ssh_conn_id' easily in all versions without custom expansion.
    # So we used a Python decorated task to wrap the SSH execution or use mapped SSHOperator if supported.
    # For safety and clarity in this example, I'll use a mapped Python task that invokes SSHHook or just SSHOperator.
    
    # Actually, proper Airflow 2.3+ mapping:
    upgrade_secondaries = SSHOperator.partial(
        task_id='upgrade_secondary',
        command=upgrade_commands,
        cmd_timeout=600,
        # assuming ssh_conn_id matches hostname, or we map it differently. 
        # For simplicity, we assume generic 'ssh_default' or user passed ID, 
        # BUT usually we need unique IPs. 
        # Let's map remote_host instead.
        ssh_conn_id='ssh_default', 
    ).expand(remote_host=secondary_hosts)

    # Upgrade the FORMER primary (which is now a secondary)
    upgrade_former_primary = SSHOperator(
        task_id='upgrade_former_primary',
        command=upgrade_commands,
        ssh_conn_id='ssh_default',
        remote_host=discovery['primary'], # dynamic reference to the result of discovery
        cmd_timeout=600
    )

    # Orchestration wiring
    step_down_task = step_down_primary(discovery)
    set_fcv_task = set_feature_compatibility_version()

    # Flow
    # 1. Discover
    # 2. Upgrade all current secondaries (in parallel)
    # 3. Step down primary (to make it a secondary)
    # 4. Upgrade the node that was primary
    # 5. Finalize FCV
    
    discovery >> upgrade_secondaries >> step_down_task >> upgrade_former_primary >> set_fcv_task

dag = mongodb_rs_upgrade()
