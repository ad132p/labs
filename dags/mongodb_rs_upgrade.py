from datetime import datetime
from airflow.sdk import dag, task
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.models import Variable
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
    def service_discovery(**context):
        """
        Connects to the MongoDB Replica Set and identifies the Primary and Secondaries.
        Performs health checks:
        1. Ensures all nodes are in healthy state (PRIMARY, SECONDARY, ARBITER).
        2. Ensures no Secondary is lagging behind more than 10 seconds.
        Returns a dict with 'primary' and 'secondaries' lists.
        """
        # Configuration - In a real scenario, fetch these from Variables or Connections
        mongo_uri = context['params'].get('mongo_uri','mongodb://root:percona@db-1:27017,db-2:27017,db-3:27017/?replicaSet=rs0')
        
        client = pymongo.MongoClient(mongo_uri)
        status = client.admin.command('replSetGetStatus')
        
        members = status['members']
        primary = None
        secondaries = []
        
        # Find Primary first to get reference optime
        primary_optime = None
        for member in members:
            if member['stateStr'] == 'PRIMARY':
                primary_optime = member['optimeDate']
                break
        
        if not primary_optime:
             raise Exception("No Primary found in Replica Set! Cluster may be unhealthy.")

        for member in members:
            name = member['name']
            state = member['stateStr']
            host = name.split(':')[0] # Assuming name is host:port
            
            # Health Check: Validate State
            if state not in ['PRIMARY', 'SECONDARY', 'ARBITER']:
                raise Exception(f"Node {name} is in unhealthy state: {state}")
            
            # Replication Lag Check for Secondaries
            if state == 'SECONDARY':
                secondary_optime = member['optimeDate']
                lag = (primary_optime - secondary_optime).total_seconds()
                
                if lag > 10:
                    raise Exception(f"Secondary {name} is lagging behind by {lag} seconds (Threshold: 10s).")
                
                secondaries.append(host)
                
            elif state == 'PRIMARY':
                primary = host

        if not primary:
            raise Exception("No Primary found in Replica Set (Logic Error)!")
            
        print(f"Service Discovery passed. Primary: {primary}, Secondaries: {secondaries}")
        
        return {
            'primary': primary,
            'secondaries': secondaries,
            'all_members': [primary] + secondaries
        }

    discovery = service_discovery()

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

    @task(multiple_outputs=False)
    def step_down_primary(topology, **context):
        """
        Connects to the Primary node and executes rs.stepDown().
        Verifies that a new primary is elected and returns its hostname.
        """
        old_primary = topology['primary']
        mongo_uri = context['params'].get('mongo_uri', 'mongodb://root:percona@db-1:27017,db-2:27017,db-3:27017/?replicaSet=rs0')
        
        # We specifically want to connect to the TOPOLOGY (any node) to see the new state
        # Connecting to the old primary might result in connection checks failing or read-only mode, 
        # but we need to find who the NEW primary is.
        client = pymongo.MongoClient(mongo_uri)
        
        print(f"Stepping down primary: {old_primary}")
        try:
            # We connect specifically to the old primary to command it to step down
            # In a real dynamic scenario, one might create a direct client to `primary_host`
            client.admin.command('replSetStepDown', 60, secondaryCatchUpPeriodSecs=10, force=True)
        except pymongo.errors.AutoReconnect:
            print("Successfully stepped down (connection closed as expected).")
        except Exception as e:
            print(f"Error executing stepDown: {e}")
            # It might have succeeded but threw an error due to connection loss
            pass
        
        # Wait for election and verify new primary
        print("Waiting for new primary to be elected...")
        for i in range(10):
            time.sleep(5)
            try:
                # We query the cluster status
                status = client.admin.command('replSetGetStatus')
                new_primary = None
                
                for member in status['members']:
                    if member['stateStr'] == 'PRIMARY':
                        new_primary = member['name'].split(':')[0]
                        break
                
                if new_primary:
                    if new_primary != old_primary:
                        print(f"New Primary elected: {new_primary}")
                        return new_primary
                    else:
                        print(f"Old primary {old_primary} is still primary. Waiting...")
                else:
                    print("No primary currently elected. Waiting...")
                    
            except Exception as e:
                print(f"Error checking status: {e}")
        
        raise Exception("Failed to elect a new primary after stepDown!")

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

    @task
    def upgrade_secondaries_sequentially(discovery_result, **context):
        from airflow.providers.ssh.hooks.ssh import SSHHook
        
        secondaries = discovery_result['secondaries']
        mongo_uri = context['params'].get('mongo_uri', 'mongodb://root:percona@db-1:27017,db-2:27017,db-3:27017/?replicaSet=rs0')
        client = pymongo.MongoClient(mongo_uri)
        
        for host in secondaries:
            print(f"Starting upgrade for secondary: {host}")
            
            # Execute Upgrade Command via SSH
            # We assume the SSH connection ID matches the host or is passed in a predictable way
            # For this example, we'll try to use 'ssh_default' but ideally it should be dynamic
            # In a real world scenario, you might have specific conn_ids per host or use the hostname in the conn
            
            try:
                # Using a generic hook for demonstration, assuming it can connect to 'host'
                # If using distinct connection IDs per host:
                # ssh_hook = SSHHook(ssh_conn_id=f"ssh_{host}") 
                # If using one key for all:
                ssh_hook = SSHHook(ssh_conn_id='ssh_default', remote_host=host)
                
                # Execute command ONLY ONCE
                stdin, stdout, stderr = ssh_hook.get_conn().exec_command(upgrade_commands, timeout=600)
                
                # Wait for command to complete and get exit status
                exit_status = stdout.channel.recv_exit_status()
                
                print(f"Upgrade output for {host}:")
                out_str = stdout.read().decode()
                err_str = stderr.read().decode()
                print(out_str)
                
                if err_str:
                    print(f"STDERR for {host}:")
                    print(err_str)
                
                if exit_status != 0:
                    raise Exception(f"Upgrade failed on {host} with status {exit_status}")
                    
            except Exception as e:
                raise Exception(f"SSH Execution failed for {host}: {e}")

            # Health Check: Wait for node to return to SECONDARY state
            print(f"Waiting for {host} to rejoin as SECONDARY...")
            max_retries = 30
            retry = 0
            while retry < max_retries:
                try:
                    status = client.admin.command('replSetGetStatus')
                    member_status = next((m for m in status['members'] if m['name'].startswith(host)), None)
                    
                    if member_status:
                        state_str = member_status['stateStr']
                        print(f"Current state of {host}: {state_str}")
                        if state_str == 'SECONDARY':
                            print(f"{host} is healthy and caught up.")
                            break
                    else:
                        print(f"Member {host} not found in status yet...")
                        
                except Exception as e:
                    print(f"Error checking status: {e}")
                
                time.sleep(10)
                retry += 1
            
            if retry >= max_retries:
                raise Exception(f"Timeout waiting for {host} to become SECONDARY")

    upgrade_secondaries = upgrade_secondaries_sequentially(discovery)

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

    def check_approval(**context):
        if Variable.get("mongodb_upgrade_proceed_fcv", default_var="no") == "yes":
            return True
        return False

    wait_for_confirmation = PythonSensor(
        task_id='wait_for_fcv_confirmation',
        python_callable=check_approval,
        mode='reschedule',
        poke_interval=60,
        timeout=3600 # 1 hour timeout
    )

    # Flow
    # 1. Discover
    # 2. Upgrade all current secondaries (in parallel)
    # 3. Step down primary (to make it a secondary)
    # 4. Upgrade the node that was primary
    # 5. Wait for Manual Confirmation
    # 6. Finalize FCV
    
    discovery >> upgrade_secondaries >> step_down_task >> upgrade_former_primary >> wait_for_confirmation >> set_fcv_task

dag = mongodb_rs_upgrade()
