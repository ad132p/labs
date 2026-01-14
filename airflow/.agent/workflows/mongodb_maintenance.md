---
description: Run MongoDB Upgrade or Rollback
---

# MongoDB Maintenance Workflows

This workflow describes how to trigger the MongoDB Replica Set upgrade and rollback DAGs using the Airflow CLI.

## Prerequisites
- Airflow environment is running.
- `ssh_default` connection is configured to access MongoDB hosts.
- `mongodb_rs_upgrade` and `mongodb_rs_rollback` DAGs are active.

## Upgrade (v7 -> v8)

1. Trigger the upgrade DAG:
   ```bash
   airflow dags trigger mongodb_rs_upgrade_v7_to_v8 --conf '{"mongo_uri": "mongodb://root:pass@host1,host2,host3/?replicaSet=rs0"}'
   ```
2. Monitor execution:
   ```bash
   # Replace run_id with the ID returned from trigger command
   airflow dags list-runs -d mongodb_rs_upgrade_v7_to_v8
   ```

## Rollback (v8 -> v7)

1. Trigger the rollback DAG:
   ```bash
   airflow dags trigger mongodb_rs_rollback_v8_to_v7 --conf '{"mongo_uri": "mongodb://root:pass@host1,host2,host3/?replicaSet=rs0"}'
   ```
   > **Note**: This will automatically handle FCV downgrade and sequential binary downgrade.

2. Monitor execution:
   ```bash
   airflow dags list-runs -d mongodb_rs_rollback_v8_to_v7
   ```
