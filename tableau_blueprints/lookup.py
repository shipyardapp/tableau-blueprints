import sys
import tableauserverclient as TSC

try:
    import errors
except BaseException:
    from . import errors


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


def get_datasource_id(server, project_id, datasource_name):
    """
    Looks up and returns the datasource_id of the datasource_name that was specified, filtered by project_id matches.
    """
    req_option = TSC.RequestOptions()
    req_option.filter.add(TSC.Filter(TSC.RequestOptions.Field.Name,
                                     TSC.RequestOptions.Operator.Equals,
                                     datasource_name))

    datasource_matches = server.datasources.get(req_options=req_option)

    # We can't filter by project_id or project_name in the initial request,
    # so we have to find all name matches and look for a project_id match.
    datasource_id = None
    for datasource in datasource_matches[0]:
        if datasource.project_id == project_id:
            datasource_id = datasource.id
    if datasource_id is None:
        print(
            f'{datasource_name} could not be found that lives in the project you specified. Please check for typos and ensure that the name(s) you provide match exactly (case sensitive)')
        sys.exit(errors.EXIT_CODE_INVALID_DATASOURCE)
    return datasource_id


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
            f'{workbook_name} could not be found in the project you specified. Please check for typos and ensure that the name(s) you provide match exactly (case sensitive)')
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
