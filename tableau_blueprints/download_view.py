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
    parser.add_argument('--workbook-name', dest='workbook_name', required=True)
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
        server.version = '3.15'
        connection = server.auth.sign_in(tableau_auth)
    except Exception as e:
        print(f'Failed to connect to Tableau.')
        print(e)
        sys.exit(EXIT_CODE_INVALID_CREDENTIALS)
    return server, connection


def validate_get_view(server, connection, view_name, workbook_name):
    """Validate workbook and returns the details of a specific view.

    :param server:
    :param connection: Tableau connection object
    :param view_name: The name of the view.
    :param workbook_name: The name of the workbook associated with the view.
    :return: view_id: details of a specific view.
    """
    try:
        view_id = None
        req_option = TSC.RequestOptions()
        req_option.filter.add(TSC.Filter(TSC.RequestOptions.Field.Name,
                                         TSC.RequestOptions.Operator.Equals,
                                         workbook_name))
        with connection:
            all_workbooks = server.workbooks.get(req_options=req_option)
            workbook_id= [workbook.id for workbook in all_workbooks[0] if workbook.name == workbook_name]
            if workbook_id[0] is not None:
                # get the workbook item
                workbook = server.workbooks.get_by_id(workbook_id[0])

                # get the view information
                server.workbooks.populate_views(workbook)

                # print information about the views for the work item
                view_info=[view.id for view in workbook.views if view.name == view_name]
            else:
                print(f'{view_name} could not be found for the given workbook {workbook_name}.'
                      f'Please check for typos and ensure that the name you provide matches exactly (case senstive)')
                sys.exit(EXIT_CODE_INVALID_RESOURCE)

    except Exception as e:
        print(f'{view_name} could not be found for the given workbook {workbook_name}.'
              f'Please check for typos and ensure that the name you provide matches exactly (case senstive)')
        print(e)
        sys.exit(EXIT_CODE_INVALID_RESOURCE)
    if len(view_info)<=0:
        print(f'{view_name} could not be found or your user does not have access. '
              f'Please check for typos and ensure that the name you provide matches exactly (case senstive)')
        sys.exit(EXIT_CODE_INVALID_RESOURCE)
    else:
        view_id=view_info[0]
    return view_id


def download_view_item(server, connection, filename, filetype, view_id, view_name):
    """to download a view from Tableau Server

    :param server:
    :param connection: Tableau connection object
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
    workbook_name = args.workbook_name
    fileoptions = args.file_options  # TODO
    server, connection = authenticate_tableau(username, password, site_id, server_url)
    view_id = validate_get_view(server, connection, view_name, workbook_name)
    # TODO
    # calling method twice, somehow the sign in context manager is not able to authenticate. Need to investigate.
    server, connection = authenticate_tableau(username, password, site_id, server_url)
    download_view_item(server, connection, filename, filetype, view_id, view_name)


if __name__ == '__main__':
    main()
