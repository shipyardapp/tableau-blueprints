import argparse
import sys
import os
import code
import shipyard_utils as shipyard

import tableauserverclient as TSC

try:
    import authorization
    import errors
except BaseException:
    from . import authorization
    from . import errors


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


def get_project_id(server, project_name):
    """
    Looks up and returns the project_id of the project_name that was specified.
    """
    req_option = TSC.RequestOptions()
    req_option.filter.add(TSC.Filter(TSC.RequestOptions.Field.Name,
                                     TSC.RequestOptions.Operator.Equals,
                                     project_name))

    project_matches = server.projects.get(req_options=req_option)
    if len(project_matches[0]) == 1:
        project_id = project_matches[0][0].id
    else:
        print(
            f'{project_name} could not be found. Please check for typos and ensure that the name you provide matches exactly (case sensitive)')
        sys.exit(errors.EXIT_CODE_INVALID_PROJECT)
    return project_id


def get_workbook_id(server, project_id, workbook_name):
    """
    Looks up and returns the workbook_id of the workbook_name that was specified, filtered by project_id matches.
    """
    req_option = TSC.RequestOptions()
    req_option.filter.add(TSC.Filter(TSC.RequestOptions.Field.Name,
                                     TSC.RequestOptions.Operator.Equals,
                                     workbook_name))

    workbook_matches = server.workbooks.get(req_options=req_option)
    # We can't filter by project_id in the initial request,
    # so we have to find all name matches and look for a project_id match.
    workbook_id = None
    for workbook in workbook_matches[0]:
        if workbook.project_id == project_id:
            workbook_id = workbook.id
    if workbook_id is None:
        print(
            f'{workbook_name} could not be found that lives in the project you specified. Please check for typos and ensure that the name(s) you provide match exactly (case sensitive)')
        sys.exit(errors.EXIT_CODE_INVALID_WORKBOOK)
    return workbook_id


def get_view_id(server, project_id, workbook_id, view_name):
    """
    Looks up and returns the view_id of the view_name that was specified, filtered by project_id AND workbook_id matches.
    """
    req_option = TSC.RequestOptions()
    req_option.filter.add(TSC.Filter(TSC.RequestOptions.Field.Name,
                                     TSC.RequestOptions.Operator.Equals,
                                     view_name))

    view_matches = server.views.get(req_options=req_option)
    # We can't filter by project_id or workbook_id in the initial request,
    # so we have to find all name matches and look for those matches.
    view_id = None
    for view in view_matches[0]:
        if view.project_id == project_id:
            if view.workbook_id == workbook_id:
                view_id = view.id
    if view_id is None:
        print(
            f'{view_name} could not be found that lives in the project and workbook you specified. Please check for typos and ensure that the name(s) you provide match exactly (case sensitive)')
        sys.exit(errors.EXIT_CODE_INVALID_VIEW)
    return view_id


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
        project_id = get_project_id(server=server, project_name=project_name)
        workbook_id = get_workbook_id(
            server=server,
            project_id=project_id,
            workbook_name=workbook_name)
        view_id = get_view_id(
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
