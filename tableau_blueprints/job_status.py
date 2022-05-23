from httprequest_blueprints import execute_request

import tableauserverclient as TSC
import argparse
import sys
import os
import pickle

EXIT_CODE_FINAL_STATUS_SUCCESS = 0
EXIT_CODE_UNKNOWN_ERROR = 3
EXIT_CODE_INVALID_CREDENTIALS = 200
EXIT_CODE_INVALID_RESOURCE = 201
EXIT_CODE_FINAL_STATUS_ERRORED = 211
EXIT_CODE_FINAL_STATUS_CANCELLED = 212
EXIT_CODE_STATUS_INCOMPLETE = 210
EXIT_CODE_JOB_NOT_FOUND = 404
EXIT_CODE_JOB_CACNELLED = 403
EXIT_CODE_GENERIC_QUERY_JOB_ERROR = 400


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--username', dest='username', required=True)
    parser.add_argument('--password', dest='password', required=True)
    parser.add_argument('--site-id', dest='site_id', required=True)
    parser.add_argument('--server-url', dest='server_url', required=True)
    parser.add_argument('--job-id', dest='job_id', required=True)
    args = parser.parse_args()
    return args


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
        connection = server.auth.sign_in(tableau_auth)
    except Exception as e:
        print(f'Failed to connect to Tableau.')
        print(e)
        sys.exit(EXIT_CODE_INVALID_CREDENTIALS)
    return server, connection


def determine_run_status(run_details_response):
    """Job status response handler.
    The finishCode indicates the status of the job: 0 for success, 1 for error, or 2 for cancelled.

    :param run_details_response:
    :return:
    """
    run_id = run_details_response.id
    if run_details_response.finish_code == 0:
        if run_details_response.progress == "Pending":
            print(f'Tableau reports that the run {run_id} in Pending status.')
            exit_code = EXIT_CODE_STATUS_INCOMPLETE
        elif run_details_response.progress == "Cancelled":
            print(f'Tableau reports that run {run_id} was cancelled.')
            exit_code = EXIT_CODE_JOB_CACNELLED
        elif run_details_response.progress == "Failed":
            print(f'Tableau reports that run {run_id} was Failed.')
            exit_code = EXIT_CODE_FINAL_STATUS_ERRORED
        elif run_details_response.progress == "InProgress":
            print(f'Tableau reports that run {run_id} is in InProgress.')
            exit_code = EXIT_CODE_STATUS_INCOMPLETE
        else:
            print(f'Tableau reports that run {run_id} was successful.')
            exit_code = EXIT_CODE_FINAL_STATUS_SUCCESS
    elif run_details_response.finish_code == 1:
        print(f'Tableau reports that the job {run_id} is exited with error.')
        exit_code = EXIT_CODE_GENERIC_QUERY_JOB_ERROR
    else:
        print(f'Tableau reports that the job {run_id} was cancelled.')
        exit_code = EXIT_CODE_STATUS_INCOMPLETE
    return exit_code


def get_job_status(server, connection, job_id):
    """Gets information about the specified job.

    :param connection:
    :param job_id: the job_id specifies the id of the job that is returned from an asynchronous task.
    :return: jobinfo : RefreshExtract, finish_code
    """
    try:
        with connection:
            jobinfo = server.jobs.get_by_id(job_id)
    except Exception as e:
        print(f'Job {job_id} Resource Not Found.')
        print(e)
        sys.exit(EXIT_CODE_JOB_NOT_FOUND)
    return jobinfo


def main():
    args = get_args()
    username = args.username
    password = args.password
    site_id = args.site_id
    server_url = args.server_url

    artifact_directory_default = f'{os.environ.get("USER")}-artifacts'

    base_folder_name = execute_request.clean_folder_name(
        f'{os.environ.get("SHIPYARD_ARTIFACTS_DIRECTORY", artifact_directory_default)}/tableau-blueprints/')

    pickle_folder_name = execute_request.clean_folder_name(
        f'{base_folder_name}/variables')

    execute_request.create_folder_if_dne(pickle_folder_name)
    pickle_file_name = execute_request.combine_folder_and_file_name(
        pickle_folder_name, 'job_id.pickle')

    if args.job_id:
        job_id = args.job_id
    else:
        with open(pickle_file_name, 'rb') as f:
            job_id = pickle.load(f)

    server, connection = authenticate_tableau(username, password, site_id, server_url)
    sys.exit(get_job_status(server, connection, job_id))


if __name__ == '__main__':
    main()
