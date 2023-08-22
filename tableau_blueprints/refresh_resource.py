import tableauserverclient as TSC
import sys
import time
import argparse
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
    parser.add_argument('--wait-for-completion', dest='wait_for_completion', default='FALSE',
                        choices={
                            'TRUE',
                            'FALSE'},
                        required=False)
    return parser.parse_args()


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
            print('A refresh or extract operation for the datasource is already underway.')
        if 'is not allowed.' in e.args[0]:
            print('Refresh or extract operation for the datasource is not allowed.')
        else:
            print('An unknown refresh or extract error occurred.')
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
            print('A refresh or extract operation for the workbook is already underway.')
        if 'is not allowed.' in e.args[0]:
            print('Refresh or extract operation for the workbook is not allowed.')
        else:
            print('An unknown refresh or extract error occurred.')
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
    wait_for_completion = shipyard.args.convert_to_boolean(args.wait_for_completion)

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

        if wait_for_completion:
            exit_code_status = job_status.determine_job_status(server, job_id)
            while exit_code_status == errors.EXIT_CODE_STATUS_INCOMPLETE:
                print("Waiting for 60 seconds before checking again")
                time.sleep(60)
                exit_code_status = job_status.determine_job_status(server, job_id)

            sys.exit(exit_code_status)

        else:
            shipyard.logs.create_pickle_file(
                artifact_subfolder_paths, 'job_id', job_id)


if __name__ == '__main__':
    main()
