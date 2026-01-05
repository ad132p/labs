from datetime import datetime
from airflow.sdk import dag, task
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
import pymongo
import time

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

@dag(
    dag_id='mongodb_rs_rollback_v8_to_v7',
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['mongodb', 'rollback', 'maintenance'],
    doc_md="""
    # MongoDB Replica Set Rollback DAG (v8 -> v7)
    
    This DAG performs a rolling rollback of a MongoDB Replica Set from version 8.0 to 7.0.
    
    ### Workflow:
    1. **Discover Members**: Connects to the cluster to identify Primary and Secondary nodes.
    2. **Downgrade FCV**: specific step to set featureCompatibilityVersion to 7.0.
    3. **Downgrade Secondaries**: Sequential execution of downgrade steps on all Secondary nodes.
    4. **Step Down Primary**: Forces the current Primary to step down.
    5. **Downgrade Former Primary**: Downgrades the node that was previously the Primary.
    
    ### Critical Requirements:
    - **FCV MUST be downgraded first** to ensure data format compatibility.
    - `pymongo` installed in the Airflow environment.
    - SSH connection IDs configured for each MongoDB host.
    """
)
def mongodb_rs_rollback():

    @task(multiple_outputs=True)
    def discover_members(**context):
        """
        Connects to the MongoDB Replica Set and identifies the Primary and Secondaries.
        """
        mongo_uri = context['params'].get('mongo_uri','mongodb://root:percona@db-1:27017,db-2:27017,db-3:27017/?replicaSet=rs0')
        
        client = pymongo.MongoClient(mongo_uri)
        status = client.admin.command('replSetGetStatus')
        
        members = status['members']
        primary = None
        secondaries = []
        
        for member in members:
            host = member['name'].split(':')[0]
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
    def set_fcv_to_7_0(**context):
        """
        Sets the FCV to 7.0 BEFORE binaries are downgraded.
        """
        mongo_uri = context['params'].get('mongo_uri', 'mongodb://root:percona@db-1:27017,db-2:27017,db-3:27017/?replicaSet=rs0')
        client = pymongo.MongoClient(mongo_uri)
        
        print("Setting featureCompatibilityVersion to 7.0...")
        # Confirm=True is often required for FCV changes in some versions/drivers
        try:
            client.admin.command('setFeatureCompatibilityVersion', '7.0', confirm=True)
        except pymongo.errors.OperationFailure as e:
            # Some versions might not need confirm=True or have different syntax, but standard is this.
            # Retry without confirm if it fails? No, confirm=True is standard for recent versions.
            print(f"Error setting FCV 7.0: {e}")
            raise e
            
        print("Successfully initiated FCV 7.0 switch. Checking status...")
        
        # Verify
        retries = 10
        while retries > 0:
            fcv_doc = client.admin.command('getParameter', featureCompatibilityVersion=1)
            fcv = fcv_doc['featureCompatibilityVersion']
            if isinstance(fcv, dict):
                fcv = fcv.get('version')
            
            print(f"Current FCV: {fcv}")
            if fcv == '7.0':
                print("FCV is confirmed 7.0")
                return
            time.sleep(2)
            retries -= 1
        
        raise Exception("Timed out waiting for FCV to become 7.0")

    fcv_task = set_fcv_to_7_0()

    # Define the downgrade command sequence
    downgrade_commands = """
    echo "Starting Rollback on $(hostname)..."
    
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
    if command -v systemctl &> /dev/null; then
        sudo systemctl stop mongod
    else
        sudo service mongod stop
    fi
    
    if [[ "$OS" == "debian" || "$OS" == "ubuntu" ]]; then
        echo "Running Debian/Ubuntu downgrade path..."
        export DEBIAN_FRONTEND=noninteractive
        
        echo "Switching Repositories..."
        sudo percona-release disable psmdb-80
        sudo percona-release enable psmdb-70 release 
        sudo apt-get update
        
        echo "Downgrading to Percona Server for MongoDB 7.0..."
        # --allow-downgrades is crucial here
        sudo apt-get install -y --allow-downgrades percona-server-mongodb
        
    elif [[ "$OS" == "rhel" || "$OS" == "rocky" || "$OS" == "centos" || "$OS" == "almalinux" ]]; then
        echo "Running RHEL/Rocky downgrade path..."
        
        echo "Switching Repositories..."
        sudo percona-release disable psmdb-80
        sudo percona-release enable psmdb-70 release
        
        echo "Downgrading to Percona Server for MongoDB 7.0..."
        # dnf downgrade handles the version switch if the higher version package is removed or replaced
        sudo dnf downgrade -y percona-server-mongodb
        
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
    def downgrade_secondaries_sequentially(discovery_result, **context):
        secondaries = discovery_result['secondaries']
        # Reuse commands string from scope
        
        mongo_uri = context['params'].get('mongo_uri', 'mongodb://root:percona@db-1:27017,db-2:27017,db-3:27017/?replicaSet=rs0')
        client = pymongo.MongoClient(mongo_uri)
        
        for host in secondaries:
            print(f"Starting downgrade for secondary: {host}")
            
            try:
                ssh_hook = SSHHook(ssh_conn_id='ssh_default', remote_host=host)
                
                # Execute command
                stdin, stdout, stderr = ssh_hook.get_conn().exec_command(downgrade_commands, timeout=600)
                
                exit_status = stdout.channel.recv_exit_status()
                
                print(f"Downgrade output for {host}:")
                out_str = stdout.read().decode()
                err_str = stderr.read().decode()
                print(out_str)
                if err_str:
                    print(f"STDERR for {host}: \n{err_str}")
                
                if exit_status != 0:
                    raise Exception(f"Downgrade failed on {host} with status {exit_status}")

            except Exception as e:
                raise Exception(f"SSH Execution failed for {host}: {e}")

            # Health Check
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

    downgrade_secondaries = downgrade_secondaries_sequentially(discovery)

    @task
    def step_down_primary(topology, **context):
        primary_host = topology['primary']
        mongo_uri = context['params'].get('mongo_uri', 'mongodb://root:percona@db-1:27017,db-2:27017,db-3:27017/?replicaSet=rs0')
        client = pymongo.MongoClient(mongo_uri)
        
        print(f"Stepping down primary: {primary_host}")
        try:
            client.admin.command('replSetStepDown', 60, secondaryCatchUpPeriodSecs=10, force=True)
        except pymongo.errors.AutoReconnect:
            print("Successfully stepped down.")
        except Exception as e:
            print(f"Error executing stepDown: {e}")
            pass
        time.sleep(15)

    step_down = step_down_primary(discovery)

    # Downgrade the former primary (now secondary)
    downgrade_former_primary = SSHOperator(
        task_id='downgrade_former_primary',
        command=downgrade_commands,
        ssh_conn_id='ssh_default',
        remote_host=discovery['primary'],
        cmd_timeout=600
    )

    # Wiring
    # 1. Discover
    # 2. Set FCV 7.0 (Critical first step)
    # 3. Downgrade Secondaries (Seq)
    # 4. Step Down (New primary (v7) takes over)
    # 5. Downgrade Old Primary
    
    discovery >> fcv_task >> downgrade_secondaries >> step_down >> downgrade_former_primary

dag = mongodb_rs_rollback()
