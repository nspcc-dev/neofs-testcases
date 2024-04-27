## Testcases structure

Tests are located under `pytest_tests` directory.

## Testcases execution

### Initial preparation

1. Fix OpenSSL ripemd160

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


2. Prepare and activate virtualenv

```shell
$ make venv.pytest
$ . venv.pytest/bin/activate
```

If you want to exit from the current venv for any reason, use the `deactivate` command.

3. Setup pre-commit hooks to run code formatters on staged files before you run a `git commit` command:

```shell
$ pre-commit install
```

Optionally you might want to integrate code formatters with your code editor to apply formatters to code files as you go:
* isort is supported by [PyCharm](https://plugins.jetbrains.com/plugin/15434-isortconnect), [VS Code](https://cereblanco.medium.com/setup-black-and-isort-in-vscode-514804590bf9). Plugins exist for other IDEs/editors as well.
* black can be integrated with multiple editors, please, instructions are available [here](https://black.readthedocs.io/en/stable/integrations/editors.html).

4. Install Allure CLI

Allure CLI installation is not an easy task, so a better option might be to run allure from
docker container (please, refer to p.2 of the next section for instructions).

To install Allure CLI you may take one of the following ways:

- Follow the [instruction](https://docs.qameta.io/allure/#_linux) from the official website
- Consult [the thread](https://github.com/allure-framework/allure2/issues/989)
- Download release from the Github
```shell
$ wget https://github.com/allure-framework/allure2/releases/download/2.18.1/allure_2.18.1-1_all.deb
$ sudo apt install ./allure_2.18.1-1_all.deb
```
You also need the `default-jre` package installed.

If none of the options worked for you, please complete the instruction with your approach.

### Run and get report

1. Binaries

By default binaries are downloaded automatically by tests, but if you place binaries under current directory, 
they will be taken from there.

Following binaries are needed:
- neofs-cli
- neofs-adm
- neofs-ir
- neofs-lens
- neofs-node
- neofs-rest-gw
- neofs-http-gw
- neo-go
- neofs-s3-authmate
- neofs-s3-gw

2. Run tests

Make sure that the virtualenv is activated, then execute the following command to run a specific test
```shell
$ pytest --alluredir my-allure-123 -s -k test_get_object_api pytest_tests/tests/object/test_object_api.py
```

If you are going to run tests several times in the same dev environment, 
you can use the `--persist-env` key to initiate keeping the test environment. 
As a result, you'll find something like `Persist env at: env_files/*` in the report. 
For example:
```shell
[MainThread] 2024-04-27 18:31:55 [INFO] Persist env at: env_files/persisted_env_awxyrbxdwu
```
After that, you can run tests faster by adding the `--load-env` flag along with 
the received value. This way you won't waste time redeploy the environment.
For example:
```shell
pytest --alluredir my-allure-234 -s pytest_tests/tests/services/rest_gate/test_rest_bearer.py --load-env env_files/persisted_env_awxyrbxdwu 
```
If anything goes wrong, first advice is to check .github/workflows/run-tests.yml, to ensure you've done all required steps.

3. Generate report

If you opted to install Allure CLI, you can generate a report using the command `allure generate`. The web representation of the report will be under `allure-report` directory:
```shell
$ allure generate my-allure-123
$ ls allure-report/
app.js  data  export  favicon.ico  history  index.html  plugins  styles.css  widgets
```

To inspect the report in a browser, run
```shell
$ allure serve my-allure-123
```

If you prefer to run allure from Docker, you can use the following command:
```shell
$ mkdir -p $PWD/allure-reports 
$ docker run -p 5050:5050 -e CHECK_RESULTS_EVERY_SECONDS=30 -e KEEP_HISTORY=1 \
    -v $PWD/my-allure-123:/app/allure-results \
    -v $PWD/allure-reports:/app/default-reports \
    frankescobar/allure-docker-service
```

Then, you can check the allure report in your browser [by this link](http://localhost:5050/allure-docker-service/projects/default/reports/latest/index.html?redirect=false)

NOTE: feel free to select a different location for `allure-reports` directory, there is no requirement to have it inside `neofs-testcases`. For example, you can place it under `/tmp` path.

# Contributing

Feel free to contribute to this project after reading the [contributing
guidelines](CONTRIBUTING.md).

Before starting to work on a certain topic, create a new issue first, describing
the feature/topic you are going to implement.


# License

- [GNU General Public License v3.0](LICENSE)

## Pytest marks

Custom pytest marks used in tests:
* `sanity` - Tests must be runs in sanity testruns.
* `smoke` - Tests must be runs in smoke testruns.

