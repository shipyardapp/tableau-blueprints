import tableauserverclient as TSC
import argparse
import sys

EXIT_CODE_FINAL_STATUS_SUCCESS = 0
EXIT_CODE_UNKNOWN_ERROR = 3
EXIT_CODE_INVALID_CREDENTIALS = 200
EXIT_CODE_INVALID_RESOURCE = 201
EXIT_CODE_FINAL_STATUS_ERRORED = 211
EXIT_CODE_FINAL_STATUS_CANCELLED = 212


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


def get_job_status(server, connection, job_id):
    """Gets information about the specified job.

    :param connection:
    :param job_id: he job_id specifies the id of the job that is returned from an asynchronous task.
    :return: jobinfo : RefreshExtract, finish_code
    """
    try:
        datasource_id = None
        with connection:
            jobinfo = server.jobs.get_by_id(job_id)
    except Exception as e:
        print(f'Job Resource Not Found.')
        print(e)
        sys.exit(EXIT_CODE_INVALID_RESOURCE)
    return jobinfo


def refresh_datasource(server, connection, datasourceobj):
    """Refreshes the data of the specified extract.

    :param connection:
    :param datasourceobj: The datasource_item specifies the data source to update.
    :return: datasource : datasource object
    """
    try:
        with connection:
            refreshed_datasource = server.datasources.refresh(datasourceobj)
            print(f'Datasource refresh trigger successful.')
    except Exception as e:
        print(f'Refresh error or Extract operation for the datasource is not allowed.')
        print(e)
        sys.exit(EXIT_CODE_INVALID_RESOURCE)

    return refreshed_datasource.id


def main():
    args = get_args()
    username = args.username
    password = args.password
    site_id = args.site_id
    server_url = args.server_url
    job_id = args.job_id
    server, connection = authenticate_tableau(username, password, site_id, server_url)
    get_job_status(server, connection, job_id)


if __name__ == '__main__':
    main()
