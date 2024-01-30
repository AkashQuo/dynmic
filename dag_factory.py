import sys
import importlib
from datetime import datetime, timedelta
from airflow.models import DAG

sys.path.append('/usr/local/airflow')
from dag_common.dag_creation import model_factory
from dag_common.dag_creation import task_factory
from settings.sensitive_settings import PLATFORM_CREDENTIALS, HERMES_CREDENTIALS, MCARE_DAG_TYPE


DEFAULT_ARGS_TEMPLATE = {
    'owner': 'airflow',
    'start_date': datetime.today() - timedelta(days=7),
    'email': [],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
}


def create(config):
    """
    :param config: a dict containing values for dag, task, and model args
    :return: a DAG instance with the specified tasks assigned to it
    """
    client_id = config.get('client_id')
    dag_id = config.get('dag_id')
    # cron tab
    cron_tab = None
    utc_pacific_hour_diff = (datetime.utcnow() - datetime.now()) / 3600
    if utc_pacific_hour_diff.seconds == 8:
        cron_tab = config.get('pst_cron_tab')
    elif utc_pacific_hour_diff.seconds == 7:
        cron_tab = config.get('pdt_cron_tab')
    # api credentials
    dag_type = config.get("dag_type")
    if config.get("skip_credentials") != "true":
        if isinstance(dag_type, str) and dag_type.lower() == MCARE_DAG_TYPE:
            # if mcare dag then get hermes credentials
            config['username'] = HERMES_CREDENTIALS[client_id]['username']
            config['password'] = HERMES_CREDENTIALS[client_id]['password']
        else:
            # if not mcare dag then get platform credentials
            config['username'] = PLATFORM_CREDENTIALS[client_id]['username']
            config['password'] = PLATFORM_CREDENTIALS[client_id]['password']
            config['zip_password'] = PLATFORM_CREDENTIALS[client_id].get('zip_password', '')

    default_args = DEFAULT_ARGS_TEMPLATE.copy()
    default_args.update({
        'email': config.get('error_email') or [],
    })

    dag = DAG(
        dag_id=dag_id,
        description=config.get('description') or dag_id,
        default_args=default_args,
        schedule_interval=cron_tab,
        catchup=False,
        max_active_runs=1
    )

    task_names = config.get('task_names', [])
    # create models needed by tasks
    for task_name in task_names:
        for model_name in task_factory.get_op_params(task_name):
            if not config.get(model_name):
                dependencies = model_factory.get_model_dependencies(model_name)
                # create dependency models before creating model directly used by task
                for dependency_name in dependencies:
                    if not config.get(dependency_name):
                        config[dependency_name] = model_factory.create(dependency_name, config)

                config[model_name] = model_factory.create(model_name, config)

    # import custom method patches
    imports = config.get('imports', [])
    for import_config in imports:
        patchable = config.get(import_config.get('patchable'))
        if patchable:
            import_path = import_config.get('import_path')
            imported_module = importlib.import_module(import_path)
            for method_name in import_config.get('methods', []):
                method = getattr(imported_module, method_name)
                setattr(patchable, method_name, method.__get__(patchable, imported_module))

    with dag:
        tasks = []
        for task_name in task_names:
            task = task_factory.create(task_name, config)
            task.dag = dag
            tasks.append(task)

        # NOTE: this does not support branched workflows
        for i in range(len(tasks) - 1):
            tasks[i].set_downstream(tasks[i + 1])

    return dag
