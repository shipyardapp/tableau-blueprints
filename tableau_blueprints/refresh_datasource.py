from httprequest_blueprints import execute_request

import tableauserverclient as TSC
import argparse
import sys
import json
import time
import os
import pickle

# Handle import difference between local and github install
try:
    import job_status
except BaseException:
    from . import job_status

EXIT_CODE_FINAL_STATUS_SUCCESS = 0
EXIT_CODE_UNKNOWN_ERROR = 3
EXIT_CODE_INVALID_CREDENTIALS = 200
EXIT_CODE_INVALID_RESOURCE = 201
EXIT_CODE_JOB_NOT_FOUND = 404
EXIT_CODE_JOB_CACNELLED = 403
EXIT_CODE_GENERIC_QUERY_JOB_ERROR = 400


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--username', dest='username', required=True)
    parser.add_argument('--password', dest='password', required=True)
    parser.add_argument('--site-id', dest='site_id', required=True)
    parser.add_argument('--server-url', dest='server_url', required=True)
    parser.add_argument('--datasource-name', dest='datasource_name', required=True)
    parser.add_argument('--project-name', dest='project_name', required=True)
    parser.add_argument('--check-status', dest='check_status', default='TRUE',
                        choices={
                            'TRUE',
                            'FALSE'},
                        required=False)
    args = parser.parse_args()
    return args


def write_json_to_file(json_object, file_name):
    with open(file_name, 'w') as f:
        f.write(
            json.dumps(
                json_object,
                ensure_ascii=False,
                indent=4))
    print(f'Response stored at {file_name}')


def authenticate_tableau(username, password, site_id, server_url):
    """TSC library to sign in and sign out of Tableau Server and Tableau Online.

    :param username:The name of the user.
    :param password:The password of the user.
    :param site_id: The site_id for required datasources. ex: ffc7f88a-85a7-48d5-ac03-09ef0a677280
    :param server_url: This corresponds to the contentUrl attribute in the Tableau REST API.
    :return: connection object
    """
    try:
        tableau_auth = TSC.TableauAuth(username, password, site_id=site_id)
        server = TSC.Server(server_url, use_server_version=True)
        server.version = '3.15'
        connection = server.auth.sign_in(tableau_auth)
    except Exception as e:
        print(f'Failed to connect to Tableau.')
        print(e)
        sys.exit(EXIT_CODE_INVALID_CREDENTIALS)
    return server, connection


def get_datasource_id(server, connection, datasource_name, project_name):
    """Returns the specified data source item

    :param server:
    :param connection: Tableau connection object
    :param datasource_name: The name of the data source.
    :param project_name: The name of the project associated with the data source.
    :return: datasource : get the data source item to refresh
    """
    try:
        datasource_id = None
        with connection:
            req_option = TSC.RequestOptions()
            req_option.filter.add(TSC.Filter(TSC.RequestOptions.Field.Name,
                                             TSC.RequestOptions.Operator.Equals,
                                             datasource_name))
            all_datasources, pagination_item = server.datasources.get(req_options=req_option)
            for datasources in all_datasources:
                if datasources.name == datasource_name:
                    if datasources.project_name == project_name:
                        datasource_id = datasources.id
                        break
                    else:
                        print(f'{datasource_name} could not be found for the give project {project_name}.')
                        sys.exit(EXIT_CODE_INVALID_RESOURCE)
    except Exception as e:
        print(f'Datasource item may not be valid or Datasource must be retrieved from server first.')
        print(e)
        sys.exit(EXIT_CODE_INVALID_RESOURCE)

    if datasource_id is None:
        print(f'{datasource_name} could not be found or your user does not have access. '
              f'Please check for typos and ensure that the name you provide matches exactly (case senstive)')
        sys.exit(EXIT_CODE_INVALID_RESOURCE)

    return datasource_id


def refresh_datasource(server, connection, datasourceobj, datasource_name):
    """Refreshes the data of the specified extract.

    :param server:
    :param connection: Tableau connection object
    :param datasourceobj: The datasource_item specifies the data source to update.
    :param datasource_name: The name of the data source.
    :return: refreshed id : refreshed object
    """

    try:
        with connection:
            refreshed_datasource = server.datasources.refresh(datasourceobj)
            print(f'Datasource {datasource_name} was successfully triggered.')
    except Exception as e:
        print(f'Refresh error or Extract operation for the datasource is not allowed.')
        print(e)
        sys.exit(EXIT_CODE_INVALID_RESOURCE)

    return refreshed_datasource


def main():
    args = get_args()
    username = args.username
    password = args.password
    site_id = args.site_id
    server_url = args.server_url
    datasource_name = args.datasource_name
    project_name = args.project_name
    check_status = execute_request.convert_to_boolean(args.check_status)
    server, connection = authenticate_tableau(username, password, site_id, server_url)
    datasource_id = get_datasource_id(server, connection, datasource_name, project_name)

    # TODO
    # calling method twice, somehow the sign in context manager is not able to authenticate. Need to investigate.
    server, connection = authenticate_tableau(username, password, site_id, server_url)
    refreshed_datasource = refresh_datasource(server, connection, datasource_id, datasource_name)
    job_id = refreshed_datasource.id

    artifact_directory_default = f'{os.environ.get("USER")}-artifacts'
    base_folder_name = execute_request.clean_folder_name(
        f'{os.environ.get("SHIPYARD_ARTIFACTS_DIRECTORY", artifact_directory_default)}/tableau-blueprints/')

    folder_name = f'{base_folder_name}/responses',
    file_name = f'job_{job_id}_response.json'

    combined_name = execute_request.combine_folder_and_file_name(folder_name, file_name)
    # save the refresh response . check for json
    write_json_to_file(refreshed_datasource, combined_name)

    pickle_folder_name = execute_request.clean_folder_name(
        f'{base_folder_name}/variables')
    execute_request.create_folder_if_dne(pickle_folder_name)
    pickle_file_name = execute_request.combine_folder_and_file_name(
        pickle_folder_name, 'job_id.pickle')
    with open(pickle_file_name, 'wb') as f:
        pickle.dump(job_id, f)

    if check_status:
        try:
            # `wait_for_job` will throw if the job isn't executed successfully
            server.jobs.wait_for_job(refreshed_datasource)
            time.sleep(30)
            server, connection = authenticate_tableau(username, password, site_id, server_url)
            sys.exit(job_status.get_job_status(server, connection, job_id))
        except:
            print(f'Tableau reports that the job {job_id} is exited as incomplete.')
            sys.exit(EXIT_CODE_GENERIC_QUERY_JOB_ERROR)


if __name__ == '__main__':
    main()
