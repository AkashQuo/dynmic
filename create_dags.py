import sys
import os
import json

sys.path.append('/usr/local/airflow')
from dag_common.dag_creation import dag_factory

CONFIG_DIR = '/home/akash/Desktop/mpulse/airflow-dags/settings/json_dags_settings/_dynamic/'


def create_dags():
    configs = []
    dag_ids = []
    duplicate_dag_ids = set()

    for filename in os.listdir(CONFIG_DIR):
        file_path = os.path.join(CONFIG_DIR, filename)
        with open(file_path) as config_file:
            try:
                config = json.load(config_file)
                dag_id = config['dag_id']
                # store the file path for error logging
                config['config_file_path'] = file_path
                configs.append(config)

                if dag_id in dag_ids:
                    # track duplicate dag ids
                    duplicate_dag_ids.add(dag_id)
                else:
                    # track encountered dag ids
                    dag_ids.append(dag_id)
            except Exception as e:
                print('DAG CREATION ERROR - Config: {0}, Error: {1}'.format(file_path, e))

    for config in configs:
        try:
            dag_id = config['dag_id']
            if dag_id in duplicate_dag_ids:
                raise RuntimeError('Multiple config files found with DAG ID "{}"'.format(dag_id))
            else:
                dag = dag_factory.create(config)
                globals()[dag.dag_id] = dag
                print('DAG CREATION SUCCESS: {}'.format(dag.dag_id))
        except Exception as e:
            print('DAG CREATION ERROR - Config: {0}, Error: {1}'.format(config['config_file_path'], e))


create_dags()
