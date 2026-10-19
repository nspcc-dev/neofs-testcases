This repo contains functional integration tests for [NeoFS](https://github.com/nspcc-dev). 
Tests can be found under `pytest_tests/tests` directory.
We use [pytest](https://docs.pytest.org/en/stable/).

## How to get started?

0. Fix OpenSSL ripemd160

Hashlib uses OpenSSL for ripemd160 and apparently OpenSSL disabled some older crypto algos around version 3.0
in November 2021.
All the functions are still there but require manual enabling. See https://github.com/openssl/openssl/issues/16994

But we use ripemd160 for tests.
For ripemd160 to be supported, make sure that the config file `/usr/lib/ssl/openssl.cnf` contains following lines:

```
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
```

There is also a script that does this for you - `sudo python ./tools/src/openssl_config_fix.py`

1. Create and activate venv with dependencies from requirements.txt. We've put some basics into the makefile target:
```shell
$ make venv.pytest
$ . venv.pytest/bin/activate
```
But you can use any other way you want to create venv. 
If you don't know what venv is, please, check - https://docs.python.org/3/library/venv.html

2. Just run the test - `pytest -s -k test_get_object_api pytest_tests/tests/object/test_object_api.py`

3. Everything should work out of the box, if not - check the rest of this README. If nothing useful there - feel free to open a github issue. 

## Allure report

For reporting we use allure report and if you want to get it, take the following steps:

1. Run a test with `--alluredir` parameter to specify a dir where allure report json files will be saved.

```shell
pytest --alluredir my-allure-234 -s pytest_tests/tests/services/rest_gate/test_rest_bearer.py 
```

2. After the test you will see a bunch of json files inside a directory from `--alluredir`. 
Now we need to convert them into nice and pretty web page. To do this you need allure cli:

Installation for Linux: [instruction](https://allurereport.org/docs/install-for-linux/#install-from-a-deb-package)
Installation for macOS: [instruction](https://allurereport.org/docs/install-for-linux/#install-from-homebrew)

3. To generate the report:

```shell
$ allure serve my-allure-123
```
You will be redirected to your browser and a web page with allure report will be opened.

## Working with a test NeoFS environment

Tests deploy test environment out of a set of binaries and run them as a separate processes. 
By default these binaries are downloaded automatically. 
List of all required binaries with corresponding URLs can be found here - `neofs-testlib/neofs_testlib/env/templates/neofs_env_config.yaml`.
To use a patched binary for tests just place it to the repo root directory. 
Config files for binaries are located here - `neofs-testlib/neofs_testlib/env/templates`.

Live logs are printed while the tests run. The default level is `INFO`. Pass `--log-cli-level` to change it:

| Level | What is printed |
| --- | --- |
| `DEBUG` | Every command and its output, config generation, waits, plus everything below |
| `INFO` | Test progress and environment summaries (the default) |
| `ENV` | Environment summaries, warnings and errors |
| `WARNING` | Warnings and errors |

Environment summaries are printed in green: endpoints, config paths, stdout and stderr files, the local CA, and the persisted env directory. A full summary is printed once the environment is up, and again when you reload it with `--load-env`.

```shell
pytest -s pytest_tests/tests/s3/test_s3_bucket.py --log-cli-level=ENV
pytest -s pytest_tests/tests/s3/test_s3_bucket.py --log-cli-level=DEBUG
```

If you are going to run tests several times in the same dev environment, 
you can use the `--persist-env` key to initiate keeping the test environment. 
As a result, you'll find something like `Persist env at: env_files/*` in the report. 
For example:
```shell
[MainThread] 2024-04-27 18:31:55 [ ENV] Persist env at: env_files/persisted_env_awxyrbxdwu
```
After that, you can run tests faster by adding the `--load-env` flag along with 
the received value. This way you won't waste time redeploy the environment.
For example:
```shell
pytest -s pytest_tests/tests/services/rest_gate/test_rest_bearer.py --load-env env_files/persisted_env_awxyrbxdwu 
```

One of the storage nodes serves TLS, so its certificate has to be trusted by clients. On macOS this means
adding it to the keychain, therefore the first local run asks for your password. It happens only once,
all the following runs reuse the same certificate.

If for debug purposes it is needed to provide a custom config for S3 GW, REST GW, Storage Nodes, Inner Ring Nodes or Main Chain nodes,
it can be done via following env vars:
```
S3_GW_CONFIG_PATH
REST_GW_CONFIG_PATH
SN1_CONFIG_PATH
SN2_CONFIG_PATH
SN3_CONFIG_PATH
SN4_CONFIG_PATH
IR1_CONFIG_PATH
IR2_CONFIG_PATH
IR3_CONFIG_PATH
IR4_CONFIG_PATH
MAINCHAIN_CONFIG_PATH
```
Full path to a custom config file should be specified. A gateway config is used as-is, so it must already contain this environment's listen address, wallet, storage-node peers and, for S3, the TLS certificate and key.

`NEOFS_S3_GW_BIN` and `NEOFS_REST_GW_BIN` select the gateway binary for a fresh deploy. The defaults are `./neofs-s3-gw` and `./neofs-rest-gw` in the repo root.

```shell
NEOFS_S3_GW_BIN=/path/to/neofs-s3-gw pytest -s pytest_tests/tests/s3/test_s3_bucket.py --persist-env
NEOFS_REST_GW_BIN=/path/to/neofs-rest-gw pytest -s pytest_tests/tests/services/rest_gate/test_rest_options.py --persist-env
```

To retry a failed test against a binary and config of your own, without redeploying the chain and storage nodes:

1. Run the test with `--persist-env`. The log prints `S3 GW Config path` or `REST GW Config path`. That generated file has the listen address, wallet, TLS certificate and key (S3), FS-chain endpoint, and storage-node peers.
2. Stop that gateway process.
3. Copy those values into your config and keep the same listen address. `--load-env` keeps calling the original address.
4. Start your binary with that config. Do not export `S3_GW_*` or `REST_GW_*` in this shell: the gateway treats them as config overrides.

```shell
/path/to/neofs-s3-gw --config /path/to/custom-s3.yaml
/path/to/neofs-rest-gw --config /path/to/custom-rest.yaml
```

5. Rerun with `--load-env`. This run does not start the gateway; it uses the process from the previous step.

```shell
pytest -s pytest_tests/tests/s3/test_s3_bucket.py --load-env env_files/persisted_env_awxyrbxdwu
pytest -s pytest_tests/tests/services/rest_gate/test_rest_options.py --load-env env_files/persisted_env_awxyrbxdwu
```

## pytest marks

Custom pytest marks used in tests:
* `sanity` - a short subset of tests to ensure basic NeoFS functionality works.
* `simple` - a subset of tests that use simple (small) objects
* `complex` - a subset of tests that use complex (big) objects

To run tests only with `complex` objects:
```
pytest -m 'complex and not simple' pytest_tests/tests
```
To run tests only with `simple` objects:
```
pytest -m 'simple and not complex' pytest_tests/tests
```

## Tests/Libraries structure

Tests are located under `pytest_tests/tests`.

Different libraries used by tests are located under `pytest_tests/lib/` and `neofs_testlib/`.

At this moment, there is no logic behind libraries location. 
But it was assumed that common python libraries that can be useful in other projects should be placed under `neofs_testlib/`.
Other projects (e.g. [s3-tests](https://github.com/nspcc-dev/s3-tests/)) use git submodules to copy the whole repo.

## Code formatter/linter

We use [ruff](https://docs.astral.sh/ruff/). All PRs are automatically checked. 
So, please, install it in your IDE to properly format the code.
We also have some pre-commit hooks to run ruff on staged files before you run a `git commit` command, install them with:

```shell
$ pre-commit install
```

## Github Actions

Main action is located here - `.github/workflows/system-tests.yml`.
Be careful with updating since it is used throughout `https://github.com/nspcc-dev`. Be sure that all dependent workflows work. 

# Contributing

Feel free to contribute to this project after reading the [contributing
guidelines](CONTRIBUTING.md).

Before starting to work on a certain topic, create a new issue first, describing
the feature/topic you are going to implement.

# License

- [GNU General Public License v3.0](LICENSE)
