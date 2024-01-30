import sys
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator

sys.path.append('/usr/local/airflow')
import dag_common.dag_creation.legacy_tasks as legacy_tasks

TASKS_DATA = {
    # tasks using the legacy AudienceReport, EventReport and WorkflowReport templates
    'legacy_check_data_on_east_sftp': {
            'operator': ShortCircuitOperator,
            'task_kwargs': {
                    'task_id': 'check_data_on_east_sftp',
                    'python_callable': legacy_tasks.check_data_on_east_sftp,
                    'provide_context': False,
                    'op_args': ["east_sftp", "file_type"],
                    'op_kwargs': {},
            },
        },
    'legacy_first_monday': {
        'operator': ShortCircuitOperator,
        'task_kwargs': {
                'task_id': 'is_first_monday',
                'python_callable': legacy_tasks.is_first_monday,
                'provide_context': True,
                'templates_dict': {'run_id': '{{ run_id }}'},
                'op_args': [],
                'op_kwargs': {},
        },
    },
    'legacy_time_check': {
        'operator': ShortCircuitOperator,
        'task_kwargs': {
                'task_id': 'time_check',
                'python_callable': legacy_tasks.time_check,
                'provide_context': True,
                'templates_dict': {'run_id': '{{ run_id }}'},
                'op_args': [
                    'days_range',
                    'time_range',
                    'time_zone'
                ],
                'op_kwargs': {},
        },
    },
    'legacy_get_files': {
        'operator': PythonOperator,
        'task_kwargs': {
            'task_id': 'get_files',
            'python_callable': legacy_tasks.get_files,
            'provide_context': False,
            'op_args': ['legacy_audience'],
            'op_kwargs': {},
        },
    },
    'legacy_get_files_workflow': {
        'operator': PythonOperator,
        'task_kwargs': {
            'task_id': 'get_files',
            'python_callable': legacy_tasks.get_files,
            'provide_context': False,
            'op_args': ['legacy_workflow'],
            'op_kwargs': {},
        },
    },
    'legacy_get_files_from_east_to_west_sftp': {
        'operator': PythonOperator,
        'task_kwargs': {
            'task_id': 'get_files',
            'python_callable': legacy_tasks.get_files_from_east_to_west_sftp,
            'provide_context': False,
            'op_args': ['legacy_audience'],
            'op_kwargs': {},
        },
    },
    'legacy_get_zipped_files': {
        'operator': PythonOperator,
        'task_kwargs': {
            'task_id': 'get_files',
            'python_callable': legacy_tasks.get_zipped_files,
            'provide_context': False,
            'op_args': ['legacy_audience'],
            'op_kwargs': {},
        },
    },
    'legacy_audience_api': {
        'operator': PythonOperator,
        'task_kwargs': {
            'task_id': 'audience_api',
            'python_callable': legacy_tasks.audience_api,
            'provide_context': True,
            'op_args': ['legacy_audience'],
            'op_kwargs': {},
        },
    },
    'legacy_audience_api_with_subscribe': {
        'operator': PythonOperator,
        'task_kwargs': {
            'task_id': 'audience_api',
            'python_callable': legacy_tasks.audience_api_with_subscribe,
            'provide_context': True,
            'op_args': ['legacy_audience'],
            'op_kwargs': {},
        },
    },
    'legacy_audience_api_for_valid_files': {
        'operator': PythonOperator,
        'task_kwargs': {
            'task_id': 'audience_api',
            'python_callable': legacy_tasks.audience_api_for_valid_files,
            'provide_context': True,
            'op_args': ['legacy_audience', 'subscribe_member'],
            'op_kwargs': {},
        },
    },
    'legacy_audience_api_with_single_report': {
        'operator': PythonOperator,
        'task_kwargs': {
            'task_id': 'audience_api',
            'python_callable': legacy_tasks.audience_api_with_single_report,
            'provide_context': True,
            'op_args': ['legacy_audience'],
            'op_kwargs': {},
        },
    },
    'legacy_audience_api_with_single_report_and_subscribe': {
        'operator': PythonOperator,
        'task_kwargs': {
            'task_id': 'audience_api',
            'python_callable': legacy_tasks.audience_api_with_single_report_and_subscribe,
            'provide_context': True,
            'op_args': ['legacy_audience'],
            'op_kwargs': {},
        },
    },
    'legacy_audience_api_with_error_reason': {
        'operator': PythonOperator,
        'task_kwargs': {
            'task_id': 'audience_api',
            'python_callable': legacy_tasks.audience_api_with_error_reason,
            'provide_context': True,
            'op_args': ['legacy_audience'],
            'op_kwargs': {},
        },
    },
    'legacy_workflow_api_with_failed_report': {
        'operator': PythonOperator,
        'task_kwargs': {
            'task_id': 'workflow_api',
            'python_callable': legacy_tasks.workflow_api,
            'provide_context': True,
            'op_args': ['legacy_workflow', 'include_datestamp', 'report_datestamp_format'],
            'op_kwargs': {},
        },
    },
    'legacy_event_api': {
        'operator': PythonOperator,
        'task_kwargs': {
            'task_id': 'event_api',
            'python_callable': legacy_tasks.event_api,
            'provide_context': True,
            'op_args': ['legacy_event'],
            'op_kwargs': {},
        },
    },
    'legacy_event_api_with_failed_report': {
        'operator': PythonOperator,
        'task_kwargs': {
            'task_id': 'event_api',
            'python_callable': legacy_tasks.event_api_with_failed_report,
            'provide_context': True,
            'op_args': ['legacy_event'],
            'op_kwargs': {},
        },
    },
    'legacy_event_api_from_get_files_with_report': {
        'operator': PythonOperator,
        'task_kwargs': {
            'task_id': 'event_api',
            'python_callable': legacy_tasks.event_api_from_get_files_with_report,
            'provide_context': True,
            'op_args': ['legacy_event'],
            'op_kwargs': {},
        },
    },
    'legacy_event_api_from_audience_response': {
        'operator': PythonOperator,
        'task_kwargs': {
            'task_id': 'event_api_from_audience_response',
            'python_callable': legacy_tasks.event_api_from_audience_response,
            'provide_context': True,
            'op_args': ['legacy_event'],
            'op_kwargs': {},
        },
    },
    'legacy_event_api_from_audience_response_multiple_report_files': {
        'operator': PythonOperator,
        'task_kwargs': {
            'task_id': 'event_api_from_audience_response_multiple_report_files',
            'python_callable': legacy_tasks.event_api_from_audience_response_multiple_report_files,
            'provide_context': True,
            'op_args': ['legacy_event'],
            'op_kwargs': {},
        },
    },
    'legacy_event_api_from_audience_response_multiple_files': {
        'operator': PythonOperator,
        'task_kwargs': {
            'task_id': 'event_api_from_audience_response_multiple_files',
            'python_callable': legacy_tasks.event_api_from_audience_response_multiple_files,
            'provide_context': True,
            'op_args': ['legacy_event'],
            'op_kwargs': {},
        },
    },
    'legacy_event_api_from_audience_response_multiple_files_with_failed_report': {
            'operator': PythonOperator,
            'task_kwargs': {
                'task_id': 'event_api_from_audience_response_multiple_files_with_failed_report',
                'python_callable': legacy_tasks.event_api_from_audience_response_multiple_files_with_failed_report,
                'provide_context': True,
                'op_args': ['legacy_event'],
                'op_kwargs': {},
            },
    },
    'legacy_workflow_api_from_audience_response': {
            'operator': PythonOperator,
            'task_kwargs': {
                'task_id': 'workflow_api_from_audience_response',
                'python_callable': legacy_tasks.workflow_api_from_audience_response,
                'provide_context': True,
                'op_args': [
                    'legacy_workflow',
                    'workflow_id',
                    'workflow_header_map',
                    'workflow_identifier',
                    'drop_identifier_column'
                ],
                'op_kwargs': {},
            },
    },
    'legacy_end_workflow_from_audience_response': {
            'operator': PythonOperator,
            'task_kwargs': {
                'task_id': 'workflow_api_from_audience_response',
                'python_callable': legacy_tasks.end_workflow_from_audience_response,
                'provide_context': True,
                'op_args': [
                    'legacy_workflow',
                    'workflow_id',
                    'workflow_header_map',
                    'workflow_identifier',
                    'drop_identifier_column',
                    'drop_audience_member_id_column',
                    'workflow_report_separator'
                ],
                'op_kwargs': {},
            },
    },
    'legacy_store_report_file_on_sftp': {
        'operator': PythonOperator,
        'task_kwargs': {
            'task_id': 'store_report_file_on_sftp',
            'python_callable': legacy_tasks.store_report_file_on_sftp,
            'provide_context': True,
            'op_args': ['legacy_audience'],
            'op_kwargs': {},
        },
    },
    'legacy_store_report_file_on_sftp_workflow': {
        'operator': PythonOperator,
        'task_kwargs': {
            'task_id': 'store_report_file_on_sftp_workflow',
            'python_callable': legacy_tasks.store_report_file_on_sftp_without_datestamp,
            'provide_context': True,
            'op_args': ['legacy_workflow'],
            'op_kwargs': {
                'task_id': 'workflow_api'
            },
        },
    },
    'legacy_store_report_file_on_sftp_without_datestamp': {
        'operator': PythonOperator,
        'task_kwargs': {
            'task_id': 'store_report_file_on_sftp_without_datestamp',
            'python_callable': legacy_tasks.store_report_file_on_sftp_without_datestamp,
            'provide_context': True,
            'op_args': ['legacy_audience'],
            'op_kwargs': {
                'task_id': 'audience_api'
            },
        },
    },
    'legacy_store_report_files_including_event_report_on_sftp_without_datestamp': {
            'operator': PythonOperator,
            'task_kwargs': {
                'task_id': 'store_report_files_including_event_report_on_sftp_without_datestamp',
                'python_callable': legacy_tasks.store_report_files_including_event_report_on_sftp_without_datestamp,
                'provide_context': True,
                'op_args': ['legacy_audience'],
                'op_kwargs': {'task_id': 'event_api_from_audience_response_multiple_files_with_failed_report'},
            },
        },
    'legacy_store_zipped_report_file_on_sftp': {
        'operator': PythonOperator,
        'task_kwargs': {
            'task_id': 'store_report_file_on_sftp',
            'python_callable': legacy_tasks.store_zipped_report_file_on_sftp,
            'provide_context': True,
            'op_args': ['legacy_audience'],
            'op_kwargs': {},
        },
    },
    'legacy_store_report_file_on_multiple_sftps': {
        'operator': PythonOperator,
        'task_kwargs': {
            'task_id': 'store_report_file_on_multiple_sftps',
            'python_callable': legacy_tasks.store_report_file_on_multiple_sftps,
            'provide_context': True,
            'op_args': ['report_archive_configs'],
            'op_kwargs': {},
        },
    },
    'legacy_store_report_file_on_multiple_sftps_without_date_timestamp': {
        'operator': PythonOperator,
        'task_kwargs': {
            'task_id': 'store_report_file_on_multiple_sftps_without_date_timestamp',
            'python_callable': legacy_tasks.store_report_file_on_multiple_sftps_without_date_timestamp,
            'provide_context': True,
            'op_args': ['report_archive_configs'],
            'op_kwargs': {},
        },
    },
    'legacy_store_zipped_report_file_on_multiple_sftps': {
        'operator': PythonOperator,
        'task_kwargs': {
            'task_id': 'store_report_file_on_multiple_sftps',
            'python_callable': legacy_tasks.store_zipped_report_file_on_multiple_sftps,
            'provide_context': True,
            'op_args': [
                'legacy_audience',
                'report_archive_configs',
            ],
            'op_kwargs': {},
        },
    },
    'legacy_store_report_file_on_multiple_sftps_from_event_api': {
        'operator': PythonOperator,
        'task_kwargs': {
            'task_id': 'store_report_file_on_multiple_sftps',
            'python_callable': legacy_tasks.store_report_file_on_multiple_sftps_from_event_api,
            'provide_context': True,
            'op_args': ['report_archive_configs'],
            'op_kwargs': {},
        },
    },
    'legacy_cleanup_files': {
        'operator': PythonOperator,
        'task_kwargs': {
            'task_id': 'cleanup_files',
            'python_callable': legacy_tasks.cleanup_files,
            'provide_context': True,
            'op_args': ['legacy_audience'],
            'op_kwargs': {
                'task_id': 'get_files'
            },
        },
    },
    'legacy_cleanup_files_workflow': {
        'operator': PythonOperator,
        'task_kwargs': {
            'task_id': 'cleanup_files_workflow',
            'python_callable': legacy_tasks.cleanup_files,
            'provide_context': True,
            'op_args': ['legacy_workflow'],
            'op_kwargs': {
                'task_id': 'get_files'
            },
        },
    },
    'legacy_cleanup_files_multiple_sftps': {
        'operator': PythonOperator,
        'task_kwargs': {
            'task_id': 'cleanup_files_multiple_sftps',
            'python_callable': legacy_tasks.cleanup_files_multiple_sftps,
            'provide_context': True,
            'op_args': [
                'legacy_audience',
                'report_archive_configs',
            ],
            'op_kwargs': {},
        },
    },
    'legacy_store_file_on_east_sftp': {
        'operator': PythonOperator,
        'task_kwargs': {
            'task_id': 'store_file_on_east_sftp',
            'python_callable': legacy_tasks.store_file_on_east_sftp,
            'provide_context': True,
            'op_args': [
                'legacy_file_transfer'
            ],
            'op_kwargs': {
                'task_id': "get_files"
            },
        },

    },

}


def get_op_params(task_name):
    """
    :param task_name: the name of the task for which to get op_args param names
    :return: the param names for op_args to be provided to the corresponding task
    """
    return TASKS_DATA[task_name]['task_kwargs']['op_args'] if TASKS_DATA.get(task_name) else []


def create(task_name, config):
    """
    :param task_name: the name of the task to be created
    :param config: a dict containing values for task args
    :return: a task instance corresponding to a TASKS_DATA key
    """
    task = None
    task_data = TASKS_DATA.get(task_name)
    if task_data:
        task_kwargs = task_data['task_kwargs'].copy()
        # replace op_args list of param names with actual arguments
        task_kwargs['op_args'] = list(map(lambda op_arg: config.get(op_arg), task_kwargs['op_args']))
        task = task_data['operator'](**task_kwargs)

    return task
