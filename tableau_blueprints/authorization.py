import sys
import tableauserverclient as TSC

EXIT_CODE_INVALID_CREDENTIALS = 200


def connect_to_tableau(
        username,
        password,
        site_id,
        server_url,
        sign_in_method):
    """TSC library to sign in and sign out of Tableau Server and Tableau Online.

    :param username:The username or access token name of the user.
    :param password:The password or access token of the user.
    :param site_id: The site_id for required datasources. ex: ffc7f88a-85a7-48d5-ac03-09ef0a677280
    :param server_url: This corresponds to the contentUrl attribute in the Tableau REST API.
    :param sign_in_method: Whether to log in with username_password or access_token.
    :return: server object, connection object
    """
    # handle the cases where the tableau server does not have a specific site
    if str(site_id).lower() == 'default': 
        site_id = ''

    if sign_in_method == 'username_password':
        tableau_auth = TSC.TableauAuth(username, password, site_id=site_id)

    if sign_in_method == 'access_token':
        tableau_auth = TSC.PersonalAccessTokenAuth(
            token_name=username,
            personal_access_token=password,
            site_id=site_id)

    try:
        # Make sure we use an updated version of the rest apis.
        server = TSC.Server(
            server_url,
            use_server_version=True,
        )
        connection = server.auth.sign_in(tableau_auth)
        print("Successfully authenticated with Tableau.")
    except Exception as e:
        print(f'Failed to connect to Tableau.')
        if sign_in_method == 'username_password':
            print('Invalid username or password. Please check for typos and try again.')
        if sign_in_method == 'access_token':
            print(
                'Invalid token name or access token. Please check for typos and try again.')
        print(e)
        sys.exit(EXIT_CODE_INVALID_CREDENTIALS)

    return server, connection
