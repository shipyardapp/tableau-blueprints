import argparse
import os
import json
import time
import platform
import pickle
import sys

# Handle import difference between local and github install
try:
    import job_status
    import refresh_datasource
except BaseException:
    from . import job_status
    from . import refresh_datasource


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


def main():
    args = get_args()

    artifact_directory_default = f'{os.environ.get("USER")}-artifacts'
    base_folder_name = '/Users/shughson/Documents/shipyardapp/blueprints/tableau-blueprints/'

if __name__ == '__main__':
    main()