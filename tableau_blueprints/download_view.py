import argparse
import sys
import os

import tableauserverclient as TSC

EXIT_CODE_FINAL_STATUS_SUCCESS = 0
EXIT_CODE_UNKNOWN_ERROR = 3
EXIT_CODE_INVALID_CREDENTIALS = 200
EXIT_CODE_INVALID_RESOURCE = 201
EXIT_CODE_FILE_WRITE_ERROR = 1021

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--username', dest='username', required=True)
    parser.add_argument('--password', dest='password', required=True)
    parser.add_argument('--site-id', dest='site_id', required=True)
    parser.add_argument('--server-url', dest='server_url', required=True)
    parser.add_argument('--view-name', dest='view_name', required=True)
    parser.add_argument('--file-type', choices=['image', 'thumbnail', 'pdf', 'csv'], type=str.lower, required=True)
    parser.add_argument('--file-name', dest='file_name', required=True)
    parser.add_argument('--file-options', dest='file_options', required=False)
    args = parser.parse_args()
    return args


def authenticate_tableau(username, password, site_id, content_url):
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
        connection = server.auth.sign_in(tableau_auth)
    except Exception as e:
        print(f'Failed to connect to Tableau.')
        print(e)
        sys.exit(EXIT_CODE_INVALID_CREDENTIALS)
    return server, connection


def validate_get_view(server, connection, view_name):
    """Returns the details of a specific view.

    :param server:
    :param connection:
    :param view_name:
    :return:
    """
    try:
        view_id = None
        with connection:
            all_views, pagination_item = server.views.get()
            for views in all_views:
                if views.name == view_name:
                    view_id = views.id
    except Exception as e:
        print(f'View item may not be valid.')
        print(e)
        sys.exit(EXIT_CODE_INVALID_RESOURCE)
    return view_id


def download_view_item(server, connection, filename, filetype, view_id, view_name):
    """to download a view from Tableau Server

    :param server:
    :param connection:
    :param filename: name of the view to be downloaded as.
    :param filetype: 'image', 'thumbnail', 'pdf', 'csv'
    :return:
    """
    file_path = os.path.dirname(os.path.realpath(__file__))
    try:
        with connection:
            views = server.views.get_by_id(view_id)
            if filetype == "image":
                server.views.populate_image(views)
                with open('./' + filename, 'wb') as f:
                    f.write(views.image)

            if filetype == "pdf":  # Populate and save the PDF data in a file
                server.views.populate_pdf(views, req_options=None)
                with open('./' + filename, 'wb') as f:
                    f.write(views.pdf)

            if filetype == "csv":
                server.views.populate_csv(views, req_options=None)
                with open('./' + filename, 'wb') as f:
                    # Perform byte join on the CSV data
                    f.write(b''.join(views.csv))

            if filetype == "thumbnail":
                server.views.populate_preview_image(views, req_options=None)
                with open('./' + filename, 'wb') as f:
                    f.write(views.preview_image)
            print(f'View {view_name} successfully saved to {file_path}')
    except OSError as e:
        print(f'Could not write file:.')
        print(e)
        sys.exit(EXIT_CODE_FILE_WRITE_ERROR)

    return True


def main():
    args = get_args()
    username = args.username
    password = args.password
    site_id = args.site_id
    server_url = args.server_url
    view_name = args.view_name
    filetype = args.file_type
    filename = args.file_name
    fileoptions = args.file_options  # TODO
    server, connection = authenticate_tableau(username, password, site_id, server_url)
    view_id = validate_get_view(server, connection, view_name)
    if view_id is not None:
        # TODO
        # calling method twice, somehow the sign in context manager is not able to authenticate. Need to investigate.
        server, connection = authenticate_tableau(username, password, site_id, server_url)
        download_view_item(server, connection, filename, filetype, view_id, view_name)
    else:
        print(f'View item may not be valid.')
        sys.exit(EXIT_CODE_INVALID_RESOURCE)


if __name__ == '__main__':
    main()
