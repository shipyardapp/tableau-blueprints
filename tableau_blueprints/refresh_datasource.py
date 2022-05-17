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
    parser.add_argument('--site_id', dest='site_id', required=True)
    parser.add_argument('--content_url', dest='content_url', required=True)
    parser.add_argument('--datasource_id', dest='datasource_id', required=True)
    args = parser.parse_args()
    return args

def authenticate_tableu(username, password, site_id, content_url):
    """TSC library to sign in and sign out of Tableau Server and Tableau Online.

    :param username:The name of the user.
    :param password:The password of the user.
    :param site_id: The site_id for required datasources. ex: ffc7f88a-85a7-48d5-ac03-09ef0a677280
    :param content_url: This corresponds to the contentUrl attribute in the Tableau REST API.
    :return: connection object
    """
    try:
        tableau_auth = TSC.TableauAuth(username, password, site_id=site_id)
        server = TSC.Server(content_url, use_server_version=True)
        server.auth.sign_in(tableau_auth)
    except Exception as e:
        print(f'Failed to connect to Tableau.')
        print(e)
        sys.exit(EXIT_CODE_INVALID_CREDENTIALS)
    return server


def get_datasource(conenction, datasourceid):
    """Returns the specified data source item.

    :param conenction:
    :param datasourceid: The datasource_item specifies the data source to update.
    :return: datasource : get the data source item to update
    """
    try:
        datasource = conenction.datasources.get_by_id(datasourceid)
    except Exception as e:
        print(f'Datasource item may not be valid or Datasource must be retrieved from server first.')
        print(e)
        sys.exit(EXIT_CODE_INVALID_RESOURCE)
    return datasource

def refresh_datasource(conenction, datasourceobj):
    """Refreshes the data of the specified extract.

    :param conenction:
    :param datasourceobj: The datasource_item specifies the data source to update.
    :return: datasource : datasource object
    """
    try:
        refreshed_datasource = conenction.datasources.refresh(datasourceobj)
        print(f'Datasource refresh trigger successful.')
    except Exception as e:
        print(f'Refresh error or Extract operation for the datasource is not allowed.')
        print(e)
        sys.exit(EXIT_CODE_INVALID_RESOURCE)

    return EXIT_CODE_FINAL_STATUS_SUCCESS


def main():
    args = get_args()
    username = args.username
    password = args.password
    site_id = args.site_id
    content_url=args.content_url
    datasource_id = args.datasource_id
    conenction =authenticate_tableu(username, password, site_id, content_url)
    datasourceobj = get_datasource(conenction, datasource_id)
    sys.exit(refresh_datasource(conenction, datasourceobj))

if __name__ == '__main__':
    main()