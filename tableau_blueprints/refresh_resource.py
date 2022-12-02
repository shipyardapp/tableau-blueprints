import tableauserverclient as TSC
import argparse
import sys
import shipyard_utils as shipyard

# Handle import difference between local and github install
try:
    import job_status
    import errors
    import authorization
    import lookup
except BaseException:
    from . import job_status
    from . import errors
    from . import authorization
    from . import lookup


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--username', dest='username', required=True)
    parser.add_argument('--password', dest='password', required=True)
    parser.add_argument('--site-id', dest='site_id', required=True)
    parser.add_argument('--server-url', dest='server_url', required=True)
    parser.add_argument(
        '--sign-in-method',
        dest='sign_in_method',
        default='username_password',
        choices={
            'username_password',
            'access_token'},
        required=False)
    parser.add_argument(
        '--workbook-name',
        dest='workbook_name',
        required=False)
    parser.add_argument(
        '--datasource-name',
        dest='datasource_name',
        required=False)
    parser.add_argument('--project-name', dest='project_name', required=True)
    parser.add_argument('--check-status', dest='check_status', default='TRUE',
                        choices={
                            'TRUE',
                            'FALSE'},
                        required=False)
    args = parser.parse_args()
    return args


def refresh_datasource(server, datasource_id, datasource_name):
    """
    Refreshes the data of the specified datasource_id.
    """

    try:
        datasource = server.datasources.get_by_id(datasource_id)
        refreshed_datasource = server.datasources.refresh(datasource)
        print(f'Datasource {datasource_name} was successfully triggered.')
    except Exception as e:
        if 'Resource Conflict' in e.args[0]:
            print(
                f'A refresh or extract operation for the datasource is already underway.')
        if 'is not allowed.' in e.args[0]:
            print(f'Refresh or extract operation for the datasource is not allowed.')
        else:
            print(f'An unknown refresh or extract error occurred.')
        print(e)
        sys.exit(errors.EXIT_CODE_REFRESH_ERROR)

    return refreshed_datasource


def refresh_workbook(server, workbook_id, workbook_name):
    """
    Refreshes the data of the specified datasource_id.
    """

    try:
        workbook = server.workbooks.get_by_id(workbook_id)
        refreshed_workbook = server.workbooks.refresh(workbook)
        print(f'Workbook {workbook_name} was successfully triggered.')
    except Exception as e:
        if 'Resource Conflict' in e.args[0]:
            print(
                f'A refresh or extract operation for the workbook is already underway.')
        if 'is not allowed.' in e.args[0]:
            print(f'Refresh or extract operation for the workbook is not allowed.')
        else:
            print(f'An unknown refresh or extract error occurred.')
        print(e)
        sys.exit(errors.EXIT_CODE_REFRESH_ERROR)

    return refreshed_workbook


def main():
    args = get_args()
    username = args.username
    password = args.password
    site_id = args.site_id
    server_url = args.server_url
    workbook_name = args.workbook_name
    datasource_name = args.datasource_name
    project_name = args.project_name
    sign_in_method = args.sign_in_method
    should_check_status = shipyard.args.convert_to_boolean(args.check_status)

    base_folder_name = shipyard.logs.determine_base_artifact_folder(
        'tableau')
    artifact_subfolder_paths = shipyard.logs.determine_artifact_subfolders(
        base_folder_name)
    shipyard.logs.create_artifacts_folders(artifact_subfolder_paths)

    server, connection = authorization.connect_to_tableau(
        username, password, site_id, server_url, sign_in_method)

    with connection:
        project_id = lookup.get_project_id(server, project_name)

        # Allowing user to provide one or the other. These will form two separate Blueprints
        # that use the same underlying script.
        if datasource_name:
            datasource_id = lookup.get_datasource_id(
                server, project_id, datasource_name)
            refreshed_datasource = refresh_datasource(
                server, datasource_id, datasource_name)
            job_id = refreshed_datasource.id
        if workbook_name:
            workbook_id = lookup.get_workbook_id(
                server, project_id, workbook_name)
            refreshed_workbook = refresh_workbook(
                server, workbook_id, workbook_name)
            job_id = refreshed_workbook.id

        if should_check_status:
            try:
                # `wait_for_job` will automatically check every few seconds
                # and throw if the job isn't executed successfully
                print('Waiting for the job to complete...')
                server.jobs.wait_for_job(job_id)
                exit_code = job_status.determine_job_status(server, job_id)
            except BaseException:
                exit_code = job_status.determine_job_status(server, job_id)
            sys.exit(exit_code)
        else:
            shipyard.logs.create_pickle_file(
                artifact_subfolder_paths, 'job_id', job_id)


if __name__ == '__main__':
    main()
