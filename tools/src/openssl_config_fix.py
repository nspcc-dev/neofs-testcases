"""
Fix OpenSSL ripemd160.

Hashlib uses OpenSSL for ripemd160 and apparently OpenSSL disabled some older crypto algos around version 3.0
in November 2021. All the functions are still there but require manual enabling.
See https://github.com/openssl/openssl/issues/16994
But we use ripemd160 for tests.
For ripemd160 to be supported, we need the openssl configuration file to contain the following lines:
openssl_conf = openssl_init

[openssl_init]
providers = provider_sect

[provider_sect]
default = default_sect
legacy = legacy_sect

[default_sect]
activate = 1

[legacy_sect]
activate = 1
"""

import logging
import os
from pathlib import Path
import tempfile


def setup_logging():
    """Initialize logging with level INFO."""
    logging.basicConfig(level=logging.INFO)


def modify_openssl_config(filename: Path):
    """Modify the openssl configuration file to support ripemd160."""

    logging.info(f'Modifying openssl config file at: {filename}')

    if not filename.is_file():
        logging.error(f'The file {filename} does not exist. Exiting.')
        return

    try:
        with open(filename, 'r') as file:
            lines = file.readlines()

        with tempfile.NamedTemporaryFile('w', delete=False) as temp_file:
            in_provider_sect = False
            in_default_sect = False
            for line in lines:
                if line.strip() == '#openssl_conf = openssl_init':
                    temp_file.write('openssl_conf = openssl_init\n')
                    logging.info('Enabled openssl_init')
                elif line.strip() == '[provider_sect]':
                    in_provider_sect = True
                    temp_file.write(line)
                elif in_provider_sect and line.strip() == 'default = default_sect':
                    temp_file.write(line)
                    temp_file.write('legacy = legacy_sect\n')
                    logging.info('Added legacy_sect to provider_sect')
                    in_provider_sect = False
                elif line.strip() == '[default_sect]':
                    in_default_sect = True
                    temp_file.write(line)
                elif in_default_sect and line.strip() == '# activate = 1':
                    temp_file.write('activate = 1\n')
                    logging.info('Activated default_sect')
                    in_default_sect = False
                else:
                    temp_file.write(line)
            temp_file.write('[legacy_sect]\n')
            temp_file.write('activate = 1\n')
            logging.info('Added and activated legacy_sect')

        os.chmod(temp_file.name, 0o644)
        os.replace(temp_file.name, str(filename))

    except (IOError, PermissionError) as e:
        logging.error(f'An error occurred while modifying {filename}: {e}')
    except Exception as e:
        logging.error(f'An unexpected error occurred: {e}')

    logging.info(f'Finished modifying openssl config file at: {filename}')


def main():
    setup_logging()
    openssl_config_file = Path('/etc/ssl/openssl.cnf')
    modify_openssl_config(openssl_config_file)


if __name__ == '__main__':
    main()
