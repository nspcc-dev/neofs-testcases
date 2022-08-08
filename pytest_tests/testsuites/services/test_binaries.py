import logging
from http import HTTPStatus
from re import match

import allure
import pytest
import requests

from common import BIN_VERSIONS_FILE
from service_helper import get_storage_service_helper

logger = logging.getLogger('NeoLogger')


@allure.title('Check binaries versions')
@pytest.mark.check_binaries
def test_binaries_versions(request):
    """
    Compare binaries versions from external source (url) and deployed on servers.
    """
    if not BIN_VERSIONS_FILE:
        pytest.skip('File with binaries and versions was not provided')

    failed_versions = {}
    environment_dir = request.config.getoption('--alluredir')
    env_data = None
    data_for_env = {}

    binaries_to_check = download_versions_info(BIN_VERSIONS_FILE)

    with allure.step('Get binaries versions from servers'):
        helper = get_storage_service_helper()
        got_versions = helper.get_binaries_version(binaries=list(binaries_to_check.keys()))

    if environment_dir:
        with open(f'{environment_dir}/environment.properties', 'r') as env_file:
            env_data = env_file.read()

    # compare versions from servers and file
    for binary, version in binaries_to_check.items():
        if binary not in got_versions:
            failed_versions[binary] = 'Can not find binary'
        if got_versions[binary] != version:
            failed_versions[binary] = f'Expected version {version}, found version {got_versions[binary]}'

        # if something missed in environment.properties file, let's add
        if env_data and binary not in env_data:
            data_for_env[binary] = got_versions[binary]

    if environment_dir and data_for_env:
        add_to_environment_properties(f'{environment_dir}/environment.properties', data_for_env)

    # create clear beautiful error with aggregation info
    if failed_versions:
        msg = '\n'.join({f'{binary}: {error}' for binary, error in failed_versions.items()})
        raise AssertionError(f'Found binaries with unexpected versions:\n{msg}')


@allure.step('Download info from {url}')
def download_versions_info(url: str) -> dict:
    binaries_to_version = {}

    response = requests.get(url)

    assert response.status_code == HTTPStatus.OK, \
        f'Got {response.status_code} code. Content {response.json()}'

    content = response.text
    assert content, f'Expected file with content, got {response}'

    for line in content.split('\n'):
        m = match('(.*)=(.*)', line)
        if not m:
            logger.warning(f'Could not get binary/version from {line}')
            continue
        bin_name, bin_version = m.group(1), m.group(2)
        binaries_to_version[bin_name] = bin_version

    return binaries_to_version


@allure.step('Update data in environment.properties')
def add_to_environment_properties(file_path: str, env_data: dict):
    with open(file_path, 'a+') as env_file:
        for env, env_value in env_data.items():
            env_file.write(f'{env}={env_value}\n')
