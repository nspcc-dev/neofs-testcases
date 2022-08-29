## Testcases structure

Tests written with PyTest Framework are located under `pytest_tests/testsuites` directory.

These tests rely on resources and utility modules that have been originally developed for Robot Framework:

`robot/resources/files` - static files that are used in tests' commands.

`robot/resources/lib/` - common Python libraries that provide utility functions used as building blocks in tests.

`robot/variables/` - constants and configuration variables for tests.

## Testcases execution

### Initial preparation

1. Install neofs-cli
    - `git clone git@github.com:nspcc-dev/neofs-node.git`
    - `cd neofs-node`
    - `make`
    - `sudo cp bin/neofs-cli /usr/local/bin/neofs-cli`

2. Install neofs-authmate
    - `git clone git@github.com:nspcc-dev/neofs-s3-gw.git`
    - `cd neofs-s3-gw`
    - `make`
    - `sudo cp bin/neofs-authmate /usr/local/bin/neofs-authmate`

3. Install neo-go
    - `git clone git@github.com:nspcc-dev/neo-go.git`
    - `cd neo-go`
    - `git checkout v0.92.0` (or the current version in the neofs-dev-env)
    - `make`
    - `sudo cp bin/neo-go /usr/local/bin/neo-go`
    or download binary from releases: https://github.com/nspcc-dev/neo-go/releases

4. Clone neofs-dev-env
`git clone git@github.com:nspcc-dev/neofs-dev-env.git`

Note that we expect neofs-dev-env to be located under
the `<testcases_root_dir>/../neofs-dev-env` directory. If you put this repo in any other place,
manually set the full path to neofs-dev-env in the environment variable `DEVENV_PATH` at this step.

5. Make sure you have installed all of the following prerequisites on your machine

```
make
python3.9
python3.9-dev
libssl-dev
```
As we use neofs-dev-env, you'll also need to install
[prerequisites](https://github.com/nspcc-dev/neofs-dev-env#prerequisites) of this repository.

### Run and get report

1. Prepare virtualenv

```
$ make venv.local-pytest
$ . venv.local-pytest/bin/activate
```

2. Install Allure CLI

Allure CLI installation is not an easy task, so a better option might be to run allure from
docker container (please, refer to p.4 of this section for instructions).

To install Allure CLI you may select one of the following ways:

- Follow the [instruction](https://docs.qameta.io/allure/#_linux) from the official website
- Consult [the thread](https://github.com/allure-framework/allure2/issues/989)
- Download release from the Github
```
$ wget https://github.com/allure-framework/allure2/releases/download/2.18.1/allure_2.18.1-1_all.deb
$ sudo apt install ./allure_2.18.1-1_all.deb
```
You also need the `default-jre` package installed.

If none of the options worked for you, please complete the instruction with your approach.

3. Run tests

In the activated virtualenv, execute the following command(s) to run a singular testsuite or all the suites in the directory
```
$ pytest --alluredir my-allure-123 pytest_tests/testsuites/object/test_object_api.py
$ pytest --alluredir my-allure-123 pytest_tests/testsuites/
```

4. Generate report

If you opted to install Allure CLI, you can generate a report using the command `allure generate`. The web representation of the report will be under `allure-report` directory:
```
$ allure generate my-allure-123
$ ls allure-report/
app.js  data  export  favicon.ico  history  index.html  plugins  styles.css  widgets
```

To inspect the report in a browser, run
```
$ allure serve my-allure-123
```

If you prefer to run allure from Docker, you can use the following command:
```
mkdir -p $PWD/allure-reports
docker run -p 5050:5050 -e CHECK_RESULTS_EVERY_SECONDS=30 -e KEEP_HISTORY=1 \
  -v $PWD/my-allure-123:/app/allure-results \
  -v $PWD/allure-reports:/app/default-reports \
  frankescobar/allure-docker-service
```

Then, you can check the allure report in your browser [by this link](http://localhost:5050/allure-docker-service/projects/default/reports/latest/index.html?redirect=false)

NOTE: feel free to select a different location for `allure-reports` directory, there is no requirement to have it inside `neofs-testcases`. For example, you can place it under `/tmp` path.

## Code style

The names of Python variables, functions and classes must comply with [PEP8](https://peps.python.org/pep-0008) rules, in particular:
* Name of a variable/function must be in snake_case (lowercase, with words separated by underscores as necessary to improve readability).
* Name of a global variable must be in UPPER_SNAKE_CASE, the underscore (`_`) symbol must be used as a separator between words.
* Name of a class must be in PascalCase (the first letter of each compound word in a variable name is capitalized).
* Names of other variables should not be ended with the underscore symbol.

Line length limit is set as 100 characters.

Imports should be ordered in accordance with [isort default rules](https://pycqa.github.io/isort/).
