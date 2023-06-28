import os
import subprocess
import argparse
from allure_combine import combine_allure

COMBINE_DIR = 'combine'
RUN_NUMBER = 'RunNumber'  # the key for the attribute
FILE_PATH = 'FilePath'  # the key for the attribute, is the path for the static page and allure report zip files
COMPLETE_FILE_NAME = 'index.html'  # file to write COMBINE_DIR/complete.html file data into
PUT_TIMEOUT = 600  # in seconds


def parse_args():
    parser = argparse.ArgumentParser(description='Process allure reports')
    parser.add_argument('--neofs_domain', required=True, type=str, help='NeoFS network domain, example: t5.fs.neo.org')
    parser.add_argument('--wallet', required=True, type=str, help='Path to the wallet')
    parser.add_argument('--cid', required=True, type=str, help='Container ID')
    parser.add_argument('--run_id', required=True, type=str, help='GitHub run ID')
    parser.add_argument('--allure_report', type=str, help='Path to generated allure report directory',
                        default='allure_report')
    return parser.parse_args()


def put_combine_result_as_static_page(directory: str, neofs_domain: str, wallet: str, cid: str, run_id: str,
                                      password: str) -> None:
    base_cmd = (
        f'NEOFS_CLI_PASSWORD={password} neofs-cli --rpc-endpoint st1.{neofs_domain}:8080 '
        f'--wallet {wallet}  object put --cid {cid} --timeout {PUT_TIMEOUT}s'
    )

    for subdir, dirs, files in os.walk(directory):
        current_dir_name = os.path.basename(subdir)
        for filename in files:
            filepath = subdir + os.sep + filename
            base_cmd_with_file = f'{base_cmd} --file {filepath} --attributes {RUN_NUMBER}={run_id},'
            if filename == 'complete.html' and current_dir_name == COMBINE_DIR:
                # allure_combine combines the Allure report and saves it as a static page under the name "complete.html"
                # Later we will write a patch in allure_combine or fork it, but for now we will rename to "index.html"
                filename = COMPLETE_FILE_NAME
                object_cmd = f'{base_cmd_with_file}{FILE_PATH}={run_id}/{filename}'
            elif current_dir_name == 'attachments' and filename.endswith('.zip'):
                # We save the logs archives as separate objects in order to make a static page small size.
                # Without this, its size will be hundreds of megabytes.
                object_cmd = (
                    f'{base_cmd_with_file}{FILE_PATH}={run_id}/data/{current_dir_name}/{filename} '
                    f'ContentType=application/zip'
                )
            else:
                # Unfortunately, for a static page, we can't collect all the test artifacts.
                # So we do only archives with logs, other important data are contained in the static page.
                continue

            print(f'Cmd: {object_cmd}')

            try:
                compl_proc = subprocess.run(object_cmd, check=True, text=True,
                                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=PUT_TIMEOUT,
                                            shell=True)

                print(f'RC: {compl_proc.returncode}')
                print(f'Output: {compl_proc.stdout}')
                print(f'Error: {compl_proc.stderr}')

            except subprocess.CalledProcessError as e:
                raise Exception(
                    f'Command failed: {e.cmd}\n'
                    f'Error code: {e.returncode}\n'
                    f'Output: {e.output}\n'
                    f'Stdout: {e.stdout}\n'
                    f'Stderr: {e.stderr}\n'
                )


def combine_report(allure_path: str) -> str:
    combine_dir = os.path.join(os.getcwd(), COMBINE_DIR)
    os.makedirs(combine_dir, exist_ok=True)

    combine_allure(
        allure_path,
        dest_folder=combine_dir,
        auto_create_folders=True,
        remove_temp_files=True,
        ignore_utf8_errors=True,
    )

    return combine_dir


def get_password() -> str:
    password = os.getenv('TEST_RESULTS_PASSWORD')
    return password


if __name__ == '__main__':
    args = parse_args()
    combine_path = combine_report(args.allure_report)
    neofs_password = get_password()

    put_combine_result_as_static_page(combine_path, args.neofs_domain, args.wallet, args.cid, args.run_id, neofs_password)
    put_combine_result_as_static_page(args.allure_report, args.neofs_domain, args.wallet, args.cid, args.run_id,
                                      neofs_password)

    print(f'See report: https://http.{args.neofs_domain}/{args.cid}/{args.run_id}/{COMPLETE_FILE_NAME}')
