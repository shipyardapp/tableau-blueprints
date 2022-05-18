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
    parser.add_argument('--datasource-name', dest='datasource_name', required=True)
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
        server.auth.sign_in(tableau_auth)
    except Exception as e:
        print(f'Failed to connect to Tableau.')
        print(e)
        sys.exit(EXIT_CODE_INVALID_CREDENTIALS)
    return server, tableau_auth


def get_datasource(server, tableau_auth, datasource_name):
    """Returns the specified data source item.

    :param tableau_auth:
    :param datasourceid: The datasource_item specifies the data source to update.
    :return: datasource : get the data source item to update
    """
    try:
        datasource_id = None
        with server.auth.sign_in(tableau_auth):
            all_datasources, pagination_item = server.datasources.get()
            for datasources in all_datasources:
                # print(datasources.name)
                if datasources.name == datasource_name:
                    # view_obj = server.datasources.get_by_id(datasourceid)
                    datasource_id = datasources.id
                    break
    except Exception as e:
        print(f'Datasource item may not be valid or Datasource must be retrieved from server first.')
        print(e)
        sys.exit(EXIT_CODE_INVALID_RESOURCE)
    return datasource_id


def refresh_datasource(server, tableau_auth, datasourceobj):
    """Refreshes the data of the specified extract.

    :param tableau_auth:
    :param datasourceobj: The datasource_item specifies the data source to update.
    :return: datasource : datasource object
    """
    try:
        with server.auth.sign_in(tableau_auth):
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
    datasource_name = args.datasource_name
    server, tableau_auth = authenticate_tableau(username, password, site_id, server_url)
    datasource_id = get_datasource(server, tableau_auth, datasource_name)
    # sys.exit(refresh_datasource(connection, datasourceobj))
    if datasource_id is not None:
        refresh_datasource(server, tableau_auth, datasource_id)
    else:
        print(f'datasource item may not be valid.')
        sys.exit(EXIT_CODE_INVALID_RESOURCE)


if __name__ == '__main__':
    main()
