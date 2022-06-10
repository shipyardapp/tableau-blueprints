import tableauserverclient as TSC
import argparse
import sys
import time
import shipyard_utils as shipyard
import code

# Handle import difference between local and github install
try:
    import job_status
    import errors
    import authorization
except BaseException:
    from . import job_status
    from . import errors
    from . import authorization


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
        '--datasource-name',
        dest='datasource_name',
        required=True)
    parser.add_argument('--project-name', dest='project_name', required=True)
    parser.add_argument('--check-status', dest='check_status', default='TRUE',
                        choices={
                            'TRUE',
                            'FALSE'},
                        required=False)
    args = parser.parse_args()
    return args


def get_project_id(server, project_name):
    """
    Looks up and returns the project_id of the project_name that was specified.
    """
    req_option = TSC.RequestOptions()
    req_option.filter.add(TSC.Filter(TSC.RequestOptions.Field.Name,
                                     TSC.RequestOptions.Operator.Equals,
                                     project_name))

    project_matches = server.projects.get(req_options=req_option)
    if len(project_matches[0]) == 1:
        project_id = project_matches[0][0].id
    else:
        print(
            f'{project_name} could not be found. Please check for typos and ensure that the name you provide matches exactly (case sensitive)')
        sys.exit(errors.EXIT_CODE_INVALID_PROJECT)
    return project_id


def get_datasource_id(server, project_id, datasource_name):
    """
    Looks up and returns the datasource_id of the datasource_name that was specified, filtered by project_id matches.
    """
    req_option = TSC.RequestOptions()
    req_option.filter.add(TSC.Filter(TSC.RequestOptions.Field.Name,
                                     TSC.RequestOptions.Operator.Equals,
                                     datasource_name))

    datasource_matches = server.datasources.get(req_options=req_option)

    # We can't filter by project_id or project_name in the initial request,
    # so we have to find all name matches and look for a project_id match.
    datasource_id = None
    for datasource in datasource_matches[0]:
        if datasource.project_id == project_id:
            datasource_id = datasource.id
    if datasource_id is None:
        print(
            f'{datasource_name} could not be found that lives in the project you specified. Please check for typos and ensure that the name(s) you provide match exactly (case sensitive)')
        sys.exit(errors.EXIT_CODE_INVALID_DATASOURCE)
    return datasource_id


def refresh_datasource(server, datasource_id, datasource_name):
    """Refreshes the data of the specified extract.

    :param server:
    :param connection: Tableau connection object
    :param datasourceobj: The datasource_item specifies the data source to update.
    :param datasource_name: The name of the data source.
    :return: refreshed id : refreshed object
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


def main():
    args = get_args()
    username = args.username
    password = args.password
    site_id = args.site_id
    server_url = args.server_url
    datasource_name = args.datasource_name
    project_name = args.project_name
    sign_in_method = args.sign_in_method
    should_check_status = shipyard.args.convert_to_boolean(args.check_status)

    base_folder_name = shipyard.logs.determine_base_artifact_folder(
        'dbtcloud')
    artifact_subfolder_paths = shipyard.logs.determine_artifact_subfolders(
        base_folder_name)
    shipyard.logs.create_artifacts_folders(artifact_subfolder_paths)

    server, connection = authorization.connect_to_tableau(
        username, password, site_id, server_url, sign_in_method)

    with connection:
        project_id = get_project_id(server, project_name)
        datasource_id = get_datasource_id(
            server, project_id, datasource_name)
        refreshed_datasource = refresh_datasource(
            server, datasource_id, datasource_name)
        job_id = refreshed_datasource.id

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
