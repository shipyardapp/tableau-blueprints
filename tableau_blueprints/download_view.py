import argparse
import sys
import shipyard_utils as shipyard

import tableauserverclient as TSC

try:
    import authorization
    import errors
    import lookup
except BaseException:
    from . import authorization
    from . import errors
    from . import lookup


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--username', dest='username', required=True)
    parser.add_argument('--password', dest='password', required=True)
    parser.add_argument(
        '--sign-in-method',
        dest='sign_in_method',
        default='username_password',
        choices={
            'username_password',
            'access_token'},
        required=False)
    parser.add_argument('--site-id', dest='site_id', required=True)
    parser.add_argument('--server-url', dest='server_url', required=True)
    parser.add_argument('--view-name', dest='view_name', required=True)
    parser.add_argument(
        '--file-type',
        dest='file_type',
        choices=[
            'png',
            'pdf',
            'csv'],
        type=str.lower,
        required=True)
    parser.add_argument(
        '--destination-file-name',
        dest='destination_file_name',
        default='output.csv',
        required=True)
    parser.add_argument(
        '--destination-folder-name',
        dest='destination_folder_name',
        default='',
        required=False)
    parser.add_argument('--file-options', dest='file_options', required=False)
    parser.add_argument('--workbook-name', dest='workbook_name', required=True)
    parser.add_argument('--project-name', dest='project_name', required=True)
    args = parser.parse_args()
    return args


def generate_view_content(server, view_id, file_type):
    """
    Given a specific view_id, populate the view and return the bytes necessary for creating the file.
    """
    view_object = server.views.get_by_id(view_id)
    if file_type == 'png':
        server.views.populate_image(view_object)
        view_content = view_object.image
    if file_type == 'pdf':
        server.views.populate_pdf(view_object, req_options=None)
        view_content = view_object.pdf
    if file_type == 'csv':
        server.views.populate_csv(view_object, req_options=None)
        view_content = view_object.csv
    return view_content


def write_view_content_to_file(
        destination_full_path,
        view_content,
        file_type,
        view_name):
    """
    Write the byte contents to the specified file path.
    """
    try:
        with open(destination_full_path, 'wb') as f:
            if file_type == 'csv':
                f.writelines(view_content)
            else:
                f.write(view_content)
        print(
            f'Successfully downloaded {view_name} to {destination_full_path}')
    except OSError as e:
        print(f'Could not write file: {destination_full_path}')
        print(e)
        sys.exit(errors.EXIT_CODE_FILE_WRITE_ERROR)


def main():
    args = get_args()
    username = args.username
    password = args.password
    site_id = args.site_id
    server_url = args.server_url
    sign_in_method = args.sign_in_method
    view_name = args.view_name
    file_type = args.file_type
    project_name = args.project_name
    workbook_name = args.workbook_name

    # Set all file parameters
    destination_file_name = args.destination_file_name
    destination_folder_name = shipyard.files.clean_folder_name(
        args.destination_folder_name)
    destination_full_path = shipyard.files.combine_folder_and_file_name(
        folder_name=destination_folder_name, file_name=destination_file_name)

    server, connection = authorization.connect_to_tableau(
        username,
        password,
        site_id,
        server_url,
        sign_in_method)

    with connection:
        project_id = lookup.get_project_id(
            server=server, project_name=project_name)
        workbook_id = lookup.get_workbook_id(
            server=server,
            project_id=project_id,
            workbook_name=workbook_name)
        view_id = lookup.get_view_id(
            server=server,
            project_id=project_id,
            workbook_id=workbook_id,
            view_name=view_name)

        view_content = generate_view_content(
            server=server, view_id=view_id, file_type=file_type)
        shipyard.files.create_folder_if_dne(
            destination_folder_name=destination_folder_name)
        write_view_content_to_file(
            destination_full_path=destination_full_path,
            view_content=view_content,
            file_type=file_type,
            view_name=view_name)


if __name__ == '__main__':
    main()
