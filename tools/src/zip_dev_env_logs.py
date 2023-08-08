import docker
import logging
import os
import zipfile
import tempfile


def setup_logging():
    """Initialize logging with level INFO."""
    logging.basicConfig(level=logging.INFO)


def save_container_logs(output_directory):
    client = docker.from_env()

    containers = client.containers.list()

    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    with tempfile.TemporaryDirectory() as temp_output_directory:
        for container in containers:
            container_name = container.name
            logging.info(f'Saving logs from container: {container_name}')

            log_content = container.logs().decode('utf-8')
            log_filename = f'{container_name}_logs.txt'
            log_file_path = os.path.join(temp_output_directory, log_filename)

            with open(log_file_path, 'w', encoding='utf-8') as log_file:
                log_file.write(log_content)

            logging.info(f'Logs from container {container_name} saved to file: {log_file_path}')

        zip_filename = os.path.join(output_directory, 'containers_logs.zip')
        with zipfile.ZipFile(zip_filename, 'w') as zip_file:
            for folder_name, subfolders, filenames in os.walk(temp_output_directory):
                for filename in filenames:
                    file_path = os.path.join(folder_name, filename)
                    zip_file.write(file_path, os.path.basename(file_path))

    logging.info(f'Containers logs saved to zip archive: {zip_filename}')


if __name__ == "__main__":
    setup_logging()
    output_directory = os.getcwd()
    save_container_logs(output_directory)
