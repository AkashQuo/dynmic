import sys

sys.path.append('/usr/local/airflow')
from templates.DagSettings import DagSettings
from templates.AudienceReport2 import AudienceReport
from templates.EventReport import EventReport
from templates.WorkflowReport import WorkflowReport
from templates.FileTransfer import FileTransfer

MODELS_DATA = {
    'legacy_dag_settings': {
        'model': DagSettings,
        'args': [
            'header_map',
            'account_id',
            'username',
            'password',
            'list_id',
            'success_email',
            'error_email',
            'dag_id',
            'client_id',
            'report_path',
            'pickup_path',
            'archive_path',
            'connection_id',
        ],
        'kwargs': [
            'conn_type',
            'identifier',
            'file_type',
            'separator',
            'batch_size',
            'zip_password',
            'east_sftp'
        ],
    },
    'legacy_audience': {
        'model': AudienceReport,
        'args': [
            'legacy_dag_settings',
        ],
        'kwargs': [],
        'dependencies': [
            'legacy_dag_settings',
        ],
    },
    'legacy_event': {
        'model': EventReport,
        'args': [
            'legacy_dag_settings',
            'event_name',
        ],
        'kwargs': [],
        'dependencies': [
            'legacy_dag_settings',
        ],
    },
    'legacy_workflow': {
        'model': WorkflowReport,
        'args': [
            'legacy_dag_settings',
            'workflow_column',
            'hermes_account_id',
            'mobile_phone',
            'audience_member_id_column',
            'logging_level'
        ],
        'kwargs': [],
        'dependencies': [
            'legacy_dag_settings',
        ],
    },
    'legacy_file_transfer': {
        'model': FileTransfer,
        'args': [
            'legacy_dag_settings',
            'east_sftp_conn_id',
            'east_coast_path',
            'emails'
        ],
        'kwargs': [],
        'dependencies': [
            'legacy_dag_settings',
        ],
    }

}


def get_model_dependencies(model_name):
    """
    :param model_name: the name of the model for which to return dependency model names
    :return: a list of dependency model names for the corresponding model
    """
    return MODELS_DATA[model_name]['dependencies'] if MODELS_DATA.get(model_name) else []


def create(model_name, config):
    """
    :param model_name: the name of the model to be created
    :param config: a dict containing values for model args and kwargs
    :return: a model instance corresponding to a MODELS_DATA key
    """
    model = None
    model_data = MODELS_DATA.get(model_name)
    if model_data:
        args = [config.get(arg) for arg in model_data['args']]
        kwargs = {}
        for kwarg in model_data['kwargs']:
            # only include explicitly provided kwargs
            if kwarg in config:
                kwargs[kwarg] = config[kwarg]

        model = model_data['model'](*args, **kwargs)

    return model
