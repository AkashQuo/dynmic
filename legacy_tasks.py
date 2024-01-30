import os
from datetime import datetime, time
from pytz import timezone
from airflow.contrib.hooks.sftp_hook import SFTPHook
from util.handled_exception_printer import print_exception_nicely
from dags.util.utils import convert_string_to_bool, is_file_empty
import util.zip_helper as zp
from croniter import croniter


# report object can be object of audience_report, event_report or workflow_report
# it will depend on the args sent from task_factory.py -> TASKS_DATA
def get_files(report_object):
    return report_object.get_files()


def get_files_from_east_to_west_sftp(report_obj):
    return report_obj.get_files_from_east_to_west_sftp()


def get_zipped_files(audience):
    local_files_path, files = audience.get_files()
    zip_password = audience.dag_settings.zip_password
    local_unzipped_files_path = []
    for local_file_path in local_files_path:
        if audience.dag_settings.file_type in local_file_path:
            local_unzipped_files_path += zp.unzip(local_file_path, zip_password)

    # Unzipped files are returned first to avoid rewriting audience_api calls
    return local_unzipped_files_path, files


def workflow_api(workflow, include_datestamp, report_datestamp_format, **context):
    local_files_path, _ = context['task_instance'].xcom_pull(task_ids='get_files')
    report_files = []
    for local_full_path in local_files_path:
        report_invalid_filename = workflow.process_file_with_failure_report(local_full_path, include_datestamp,
                                                                            report_datestamp_format)
        if report_invalid_filename:
            report_files.append(report_invalid_filename)
    return report_files


def audience_api(audience, **context):
    local_files_path, _ = context['task_instance'].xcom_pull(task_ids='get_files')
    report_files = []
    for local_full_path in local_files_path:
        report_valid_filename, report_invalid_filename = audience.process_file_for_audience_api_with_separate_reports(
            local_full_path)
        report_files.append(report_valid_filename)
        report_files.append(report_invalid_filename)
    return report_files


def audience_api_with_subscribe(audience, **context):
    local_files_path, _ = context['task_instance'].xcom_pull(task_ids='get_files')
    report_files = []
    for local_full_path in local_files_path:
        report_valid_filename, report_invalid_filename = audience.process_file_for_audience_api_with_separate_reports(
            local_full_path, True)
        report_files.append(report_valid_filename)
        report_files.append(report_invalid_filename)
    return report_files


def audience_api_for_valid_files(audience, subscribe_member, **context):
    """
    This function will skip the empty files.
    """
    local_files_path, _ = context['task_instance'].xcom_pull(task_ids='get_files')

    subscribe_member = True if subscribe_member is None else convert_string_to_bool(subscribe_member)
    report_files = []
    for local_full_path in local_files_path:

        # skip the empty file
        if is_file_empty(local_full_path):
            print(f"{local_full_path} is an empty file.")
            continue

        report_filenames = audience.process_file_for_audience_api_with_separate_reports(local_full_path,
                                                                                        subscribe_member)

        # adding report names only if it exists
        for report_file in report_filenames:
            if report_file:
                report_files.append(report_file)

    return report_files


def audience_api_with_single_report(audience, **context):
    local_files_path, _ = context['task_instance'].xcom_pull(task_ids='get_files')
    report_files = []
    for local_full_path in local_files_path:
        report = audience.process_file_for_audience_api_with_single_report(local_full_path)
        report_files.append(report)
    return report_files


def audience_api_with_single_report_and_subscribe(audience, **context):
    local_files_path, _ = context['task_instance'].xcom_pull(task_ids='get_files')
    report_files = []
    for local_full_path in local_files_path:
        report = audience.process_file_for_audience_api_with_single_report_and_subscribe_status(local_full_path, True)
        report_files.append(report)
    return report_files


def audience_api_with_error_reason(audience, **context):
    def report_chunk_operations(self, chunk, member):
        reason = ''
        status = ''
        identifier_map_val = self.dag_settings.header_map.get(self.dag_settings.identifier)
        key = member.get('id', {}).get(identifier_map_val.lower(), {}) or member.get('member_errors', {}).get(
            'member_error', {})

        if member.get('result') == 'success':
            status = 'success'
        elif member.get('result') == 'failure':
            status = 'failure'
            reason = member.get('member_errors', {}).get('member_error', {}).get('detail', {}).get('reason')

        chunk.loc[chunk[self.dag_settings.identifier] == key, 'status'] = status
        chunk.loc[chunk[self.dag_settings.identifier] == key, 'reason'] = reason

        return chunk

    audience.report_chunk_operations = report_chunk_operations.__get__(audience, audience.__class__)
    return audience_api(audience, **context)


def event_api(event, **context):
    local_files_path, _ = context['task_instance'].xcom_pull(task_ids='get_files')
    for local_full_path in local_files_path:
        event.process_file_for_event_api(local_full_path)


def event_api_from_get_files_with_report(event, **context):
    local_files_path, _ = context['task_instance'].xcom_pull(task_ids='get_files')
    report_files = []
    for local_full_path in local_files_path:
        report = event.process_file_for_event_api_with_failed_report(local_full_path)
        report_files.append(report)

    return report_files


def event_api_with_failed_report(event, **context):
    local_files_path, _ = context['task_instance'].xcom_pull(task_ids='get_files')
    report_files = context['task_instance'].xcom_pull(task_ids='audience_api')
    for local_full_path in local_files_path:
        report = event.process_file_for_event_api_with_failed_report(local_full_path)
        report_files.append(report)

    return report_files


def event_api_from_audience_response(event, **context):
    report_files = context['task_instance'].xcom_pull(task_ids='audience_api')
    if len(report_files):
        event.process_file_for_event_api(report_files[0])
    else:
        print('No Audience API success file to read from')


def event_api_from_audience_response_multiple_report_files(event, **context):
    report_files = context['task_instance'].xcom_pull(task_ids='audience_api')
    if report_files:
        for report_file in report_files:
            event.process_file_for_event_api(report_file)
    else:
        print('No Audience API success file to read from')

#new method which calls multiple files from [AIR-44]
def event_api_from_audience_response_multiple_files(event, **context):
    report_files = context['task_instance'].xcom_pull(task_ids='audience_api')
    if report_files:
        for report_file in report_files:
            if '_valid' in report_file:
            #this checks if the index has '_valid' in the string and then calls the event API
            #file names should look like 'sample1_valid.csv' or 'sample1_invalid.csv' 
                event.process_file_for_event_api(report_file)
    else:
        print('No Audience API success file to read from')


def workflow_api_from_audience_response(workflow, workflow_id, workflow_header_map, workflow_identifier,
                                        drop_identifier_column, **context):
    report_files = context['task_instance'].xcom_pull(task_ids='audience_api')

    # set drop_identifier_column = True as a default value
    if drop_identifier_column is None:
        drop_identifier_column = True
    for report_file in report_files:
        if '_valid' in report_file:
            workflow.process_workflow_api_from_audience_api(report_file, workflow_id, workflow_header_map,
                                                            workflow_identifier, drop_identifier_column)


def end_workflow_from_audience_response(workflow, workflow_id, workflow_header_map, workflow_identifier,
                                        drop_identifier_column, drop_amid_column, workflow_report_separator,
                                        **context):
    report_files = context['task_instance'].xcom_pull(task_ids='audience_api')
    # set drop_identifier_column = True as a default value
    drop_identifier_column = True if drop_identifier_column is None else convert_string_to_bool(drop_identifier_column)
    drop_amid_column = True if drop_amid_column is None else convert_string_to_bool(drop_amid_column)
    # setting workflow_header_map as empty map if there is no meta data
    workflow_header_map = workflow_header_map or {}

    for report_file in report_files:
        if '_valid' in report_file:
            workflow.end_workflow_from_audience_api(report_file, workflow_id, workflow_header_map, workflow_identifier,
                                                    drop_identifier_column, drop_amid_column, workflow_report_separator)


def event_api_from_audience_response_multiple_files_with_failed_report(event, **context):
    """
    This function performs the following:
    Hit event API for the members who have been successfully processed in the audience API phase.
    Generate an event report and returns all the report files
    """
    report_files = context['task_instance'].xcom_pull(task_ids='audience_api')
    event_report_files = []

    if report_files:
        for report_file_local_path in report_files:
            if '_valid' in report_file_local_path:
                event_report = event.process_file_for_event_api_with_failed_report(report_file_local_path)
                event_report_files.append(event_report)
    else:
        print('No Audience API success file to read from')

    return report_files + event_report_files


def store_report_file_on_sftp(audience, **context):
    report_files = context['task_instance'].xcom_pull(task_ids='audience_api')
    today = datetime.today().strftime('%Y%m%d%H%M')
    for report_file in report_files:
        try:
            filename = report_file.split('/')[-1].split('.csv')[0] + today + '.csv'
            sftp_report_full_path = audience.dag_settings.report_path + filename
            audience.server_hook.store_file(remote_full_path=sftp_report_full_path, local_full_path=report_file)
            os.remove(report_file)
        except Exception as e:
            print('Exception while storing report file on SFTP; file - {0}, exception - {1}'.format(report_file, e))


def store_report_file_on_sftp_without_datestamp(report_object, task_id="", **context):
    report_files = context['task_instance'].xcom_pull(task_ids=task_id)
    for report_file in report_files:
        try:
            filename = report_file.split('/')[-1]
            sftp_report_full_path = report_object.dag_settings.report_path + filename
            report_object.server_hook.store_file(remote_full_path=sftp_report_full_path, local_full_path=report_file)
            os.remove(report_file)
        except Exception as e:
            print('Exception while storing report file on SFTP; file - {0}, exception - {1}'.format(report_file, e))


def store_report_files_including_event_report_on_sftp_without_datestamp(audience, task_id='', **context):
    report_files = context['task_instance'].xcom_pull(task_ids=task_id)
    for report_file in report_files:
        try:
            filename = report_file.split('/')[-1]
            sftp_report_full_path = audience.dag_settings.report_path + filename
            audience.server_hook.store_file(remote_full_path=sftp_report_full_path, local_full_path=report_file)
            os.remove(report_file)
        except Exception as e:
            print('Exception while storing report file on SFTP; file - {0}, exception - {1}'.format(report_file, e))


def store_zipped_report_file_on_sftp(audience, **context):
    report_files = context['task_instance'].xcom_pull(task_ids='audience_api')
    unzipped_files, original_zip_file = context['task_instance'].xcom_pull(task_ids='get_files')
    today = datetime.today().strftime('%Y%m%d%H%M')
    if report_files:
        local_path = os.path.dirname(report_files[0])
        zip_file = original_zip_file[0].split('.zip')[0] + '-report_' + today + '.zip'
        zip_file_path = os.path.join(local_path, zip_file)
        zip_password = audience.dag_settings.zip_password
        zp.zip_multiple_files(report_files, zip_file_path, password=zip_password)
        try:
            sftp_report_full_path = audience.dag_settings.report_path + zip_file
            audience.server_hook.store_file(remote_full_path=sftp_report_full_path, local_full_path=zip_file_path)
            os.remove(zip_file_path)
        except Exception as e:
            print('Exception while storing report file on SFTP; file - {0}, exception - {1}'.format(zip_file, e))
        for report_file in report_files + unzipped_files:
            os.remove(report_file)


def store_report_file_on_multiple_sftps(report_archive_configs, **context):
    report_files = context['task_instance'].xcom_pull(task_ids='audience_api')
    today = datetime.today().strftime('%Y%m%d%H%M')
    for report_file in report_files:
        filename = report_file.split('/')[-1].split('.csv')[0] + today + '.csv'
        for report_archive_config in report_archive_configs:
            if report_archive_config.get('report_path', ''):
                connection_id = report_archive_config.get('connection_id')
                try:
                    sftp_hook = SFTPHook(ftp_conn_id=connection_id)
                    sftp_report_full_path = os.path.join(report_archive_config.get('report_path', ''), filename)
                    sftp_hook.store_file(remote_full_path=sftp_report_full_path, local_full_path=report_file)
                except Exception as e:
                    print('Exception while storing report file on SFTP; file - {}, sftp - {}, exception - {}'.format(
                        report_file, connection_id, e))

        os.remove(report_file)


def store_report_file_on_multiple_sftps_without_date_timestamp(report_archive_configs, **context):
    report_files = context['task_instance'].xcom_pull(task_ids='audience_api')
    for report_file in report_files:
        filename = report_file.split('/')[-1]
        for report_archive_config in report_archive_configs:
            if report_archive_config.get('report_path', ''):
                connection_id = report_archive_config.get('connection_id')
                try:
                    sftp_hook = SFTPHook(ftp_conn_id=connection_id)
                    sftp_report_full_path = os.path.join(report_archive_config.get('report_path', ''), filename)
                    sftp_hook.store_file(remote_full_path=sftp_report_full_path, local_full_path=report_file)
                except Exception as e:
                    print('Exception while storing report file on SFTP; file - {}, sftp - {}, exception - {}'.format(
                        report_file, connection_id, e))

        os.remove(report_file)


def store_zipped_report_file_on_multiple_sftps(audience, report_archive_configs, **context):
    report_files = context['task_instance'].xcom_pull(task_ids='audience_api')
    unzipped_files, original_zip_file = context['task_instance'].xcom_pull(task_ids='get_files')
    today = datetime.today().strftime('%Y%m%d%H%M')
    if report_files:
        local_path = os.path.dirname(report_files[0])
        zip_file = original_zip_file[0].split('.zip')[0] + '-report_' + today + '.zip'
        zip_file_path = os.path.join(local_path, zip_file)
        zip_password = audience.dag_settings.zip_password
        zp.zip_multiple_files(report_files, zip_file_path, password=zip_password)

        for report_archive_config in report_archive_configs:
            connection_id = report_archive_config.get('connection_id')
            try:
                sftp_hook = SFTPHook(ftp_conn_id=connection_id)
                sftp_report_full_path = os.path.join(report_archive_config.get('report_path', ''), zip_file)
                sftp_hook.store_file(remote_full_path=sftp_report_full_path, local_full_path=zip_file_path)
            except Exception as e:
                print_exception_nicely('Exception while storing report file on SFTP; file - {}, sftp - {},'
                                       ' exception - {}'.format(zip_file_path, connection_id, e))

        os.remove(zip_file_path)
        for file in report_files + unzipped_files:
            os.remove(file)


def store_report_file_on_multiple_sftps_from_event_api(report_archive_configs, **context):
    report_files = context['task_instance'].xcom_pull(task_ids='event_api')
    today = datetime.today().strftime('%Y%m%d%H%M')
    for report_file in report_files:
        filename = report_file.split('/')[-1].split('.csv')[0] + today + '.csv'
        for report_archive_config in report_archive_configs:
            if report_archive_config.get('report_path', ''):
                connection_id = report_archive_config.get('connection_id')
                try:
                    sftp_hook = SFTPHook(ftp_conn_id=connection_id)
                    sftp_report_full_path = os.path.join(report_archive_config.get('report_path', ''), filename)
                    sftp_hook.store_file(remote_full_path=sftp_report_full_path, local_full_path=report_file)
                except Exception as e:
                    print('Exception while storing report file on SFTP; file - {}, sftp - {}, exception - {}'.format(
                        report_file, connection_id, e))

        os.remove(report_file)


def cleanup_files(report_object, task_id="", **context):
    _, files = context['task_instance'].xcom_pull(task_ids=task_id)
    report_object.cleanup_files(files)


def cleanup_files_multiple_sftps(audience, report_archive_configs, **context):
    _, files = context['task_instance'].xcom_pull(task_ids='get_files')
    for file_name in files:
        try:
            local_full_path = os.path.join(audience.temp_dir, file_name)
            for report_archive_config in report_archive_configs:
                if report_archive_config.get('archive_path', ''):
                    connection_id = report_archive_config.get('connection_id')
                    try:
                        sftp_hook = SFTPHook(ftp_conn_id=connection_id)
                        remote_archive_file_path = os.path.join(report_archive_config.get('archive_path', ''), file_name)
                        sftp_hook.store_file(remote_full_path=remote_archive_file_path, local_full_path=local_full_path)
                        print('Archived original file')
                    except Exception as e:
                        print('Exception while archiving file on SFTP; file - {}, sftp - {}, exception - {}'.format(
                            local_full_path, connection_id, e))

            remote_input_file_path = os.path.join(audience.dag_settings.pickup_path, file_name)
            # remove the original file from the sftp server
            audience.server_hook.delete_file(remote_input_file_path)
            print('Deleted {} in SFTP'.format(remote_input_file_path))

            audience.server_hook.close_conn()
            os.remove(local_full_path)
        except Exception as e:
            print('Got exception while deleting file - {0}; Exception - {1}'.format(file_name, e))


def is_first_monday(**context):
    """
            Check if today is first Monday of month.
            :rval: bool
        """
    TODAY = datetime.today().strftime('%Y-%m-%d')
    run_id = context['templates_dict']['run_id']
    is_manual = run_id.startswith('manual__')
    print("RUN ID -> ", run_id)
    if is_manual:
        return True
    cron = croniter("0 0 * * 1#1")
    next_first_monday = cron.get_next(datetime).strftime('%Y-%m-%d')
    print("Next First Monday", next_first_monday)
    print("Today", TODAY)
    return next_first_monday == TODAY


def check_data_on_east_sftp(east_sftp, file_type):
    print('Checking data on EAST SFTP Server.')
    print('East SFTP dict received in config: {}'.format(east_sftp))
    east_sftp_hook = SFTPHook(ftp_conn_id=east_sftp.get('conn_id'))
    files = east_sftp_hook.list_directory(east_sftp.get('pickup_path'))
    files = [file for file in files if file.endswith(file_type)]
    print('Total files found for processing: {}'.format(len(files)))
    return len(files) > 0


def time_check(days_range, time_range, time_zone, **context):
    """
    Check if time is correct to run the dag.
    days_range: should be an array of size 2 with starting and ending day range. Values should be integer
                1 representing Monday and so on. Example -> [1,5] (both inclusive)
    time_range: should be an array of size 2 with starting and ending time range. values should be string with "hh:mm"
                format. Example -> ["09:00","22:00"]
    time_zone: it should be a single string value representing the timezone. Example -> "US/Eastern"
    """
    run_id = context['templates_dict']['run_id']
    is_manual = run_id.startswith('manual__')
    print("RUN ID -> ", run_id)
    if is_manual:
        return True
    try:
        now_datetime = datetime.now(timezone(time_zone))
        now_time = time(now_datetime.hour, now_datetime.minute)

        start_time_hour, start_time_minute = map(int, time_range[0].split(':'))
        end_time_hour, end_time_minute = map(int, time_range[1].split(':'))
        start_time, end_time = time(start_time_hour, start_time_minute), time(end_time_hour, end_time_minute)

        return (days_range[0] <= now_datetime.isoweekday() <= days_range[1]) and (start_time <= now_time <= end_time)
    except Exception as err:
        print(f"{err} error occured while checking the time.")
        return False


def store_file_on_east_sftp(file_transfer, task_id="", **context):
    _, files = context['task_instance'].xcom_pull(task_ids=task_id)
    file_transfer.store_file_on_east_sftp(files)
