import pytest
from unittest.mock import MagicMock, patch
import sys
import os

# Add dags to path to allow importing
sys.path.append('/home/apollo/airflow/dags')

from mongodb_rs_upgrade import mongodb_rs_upgrade

@pytest.fixture
def mock_mongo_client():
    with patch('pymongo.MongoClient') as mock:
        yield mock

@pytest.fixture
def mock_sleep():
    with patch('time.sleep') as mock:
        yield mock

def test_dag_loaded():
    dag = mongodb_rs_upgrade()
    assert dag is not None
    assert dag.dag_id == 'mongodb_rs_upgrade_v7_to_v8'
    assert len(dag.tasks) > 0

def test_discover_members_logic(mock_mongo_client):
    # Setup mock
    mock_db = MagicMock()
    mock_mongo_client.return_value = mock_db
    
    # Mock replSetGetStatus response
    mock_db.admin.command.return_value = {
        'members': [
            {'name': 'mongo1:27017', 'stateStr': 'PRIMARY'},
            {'name': 'mongo2:27017', 'stateStr': 'SECONDARY'},
            {'name': 'mongo3:27017', 'stateStr': 'SECONDARY'},
        ]
    }
    
    dag = mongodb_rs_upgrade()
    discover_task = dag.get_task('discover_members')
    
    # In Airflow SDK/2.x, python_callable is the decorated function.
    discover_func = discover_task.python_callable
    
    context = {'params': {'mongo_uri': 'test_uri'}}
    result = discover_func(**context)
    
    assert result['primary'] == 'mongo1'
    assert set(result['secondaries']) == {'mongo2', 'mongo3'}
    assert len(result['all_members']) == 3

def test_step_down_primary_logic(mock_mongo_client, mock_sleep):
    mock_db = MagicMock()
    mock_mongo_client.return_value = mock_db
    
    dag = mongodb_rs_upgrade()
    step_down_task = dag.get_task('step_down_primary')
    step_down_func = step_down_task.python_callable
    
    topology = {'primary': 'mongo1'}
    context = {'params': {'mongo_uri': 'test_uri'}}
    
    step_down_func(topology, **context)
    
    mock_db.admin.command.assert_called_with('replSetStepDown', 60, secondaryCatchUpPeriodSecs=10, force=True)
    assert mock_sleep.called

def test_set_fcv_logic(mock_mongo_client, mock_sleep):
    mock_db = MagicMock()
    mock_mongo_client.return_value = mock_db
    
    dag = mongodb_rs_upgrade()
    fcv_task = dag.get_task('set_feature_compatibility_version')
    fcv_func = fcv_task.python_callable
    
    context = {'params': {'mongo_uri': 'test_uri'}}
    
    fcv_func(**context)
    
    # Check if setFeatureCompatibilityVersion was called at least once
    # It might retry loop if we simulate NotPrimaryError, but here mock defaults to success
    mock_db.admin.command.assert_called_with('setFeatureCompatibilityVersion', '8.0')

