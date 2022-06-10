import tableauserverclient as TSC
import argparse
import sys
import os
import pickle
import shipyard_utils as shipyard
import code

try:
    import errors
    import authorization
except BaseException:
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
    parser.add_argument('--job-id', dest='job_id', required=False)
    args = parser.parse_args()
    return args


def get_job_info(server, job_id):
    """
    Gets information about the specified job_id.
    """
    try:
        job_info = server.jobs.get_by_id(job_id)
    except Exception as e:
        print(f'Job {job_id} was not found.')
        print(e)
        sys.exit(errors.EXIT_CODE_INVALID_JOB)
    return job_info


def determine_job_status(server, job_id):
    """
    Job status response handler.

    The finishCode indicates the status of the job: -1 for incomplete, 0 for success, 1 for error, or 2 for cancelled.
    """
    job_info = get_job_info(server, job_id)
    if job_info.finish_code == -1:
        if job_info.started_at is None:
            print(
                f'Tableau reports that the job {job_id} has not yet started.')
        else:
            print(
                f'Tableau reports that the job {job_id} is not yet complete.')
        exit_code = errors.EXIT_CODE_STATUS_INCOMPLETE
    elif job_info.finish_code == 0:
        print(f'Tableau reports that job {job_id} was successful.')
        exit_code = errors.EXIT_CODE_FINAL_STATUS_SUCCESS
    elif job_info.finish_code == 1:
        print(f'Tableau reports that job {job_id} errored.')
        exit_code = errors.EXIT_CODE_FINAL_STATUS_ERRORED
    elif job_info.finish_code == 2:
        print(f'Tableau reports that job {job_id} was cancelled.')
        exit_code = errors.EXIT_CODE_FINAL_STATUS_CANCELLED
    else:
        print(f'Something went wrong when fetching status for job {job_id}')
        exit_code = errors.EXIT_CODE_UNKNOWN_ERROR
    return exit_code


def main():
    args = get_args()
    username = args.username
    password = args.password
    site_id = args.site_id
    server_url = args.server_url
    sign_in_method = args.sign_in_method

    base_folder_name = shipyard.logs.determine_base_artifact_folder(
        'dbtcloud')
    artifact_subfolder_paths = shipyard.logs.determine_artifact_subfolders(
        base_folder_name)
    shipyard.logs.create_artifacts_folders(artifact_subfolder_paths)

    if args.job_id:
        job_id = args.job_id
    else:
        job_id = shipyard.logs.read_pickle_file(
            artifact_subfolder_paths, 'job_id')

    server, connection = authorization.connect_to_tableau(
        username, password, site_id, server_url, sign_in_method)

    with connection:
        # job_info = get_job_info(server, job_id)
        job_status = determine_job_status(server, job_id)
        sys.exit(job_status)


if __name__ == '__main__':
    main()
