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

## Robot Framework

### Run

1. Prepare virtualenv

```
$ make venv.localtest
$ . venv.localtest/bin/activate
```

2. Run tests

In the activated virtualenv, execute the following command(s) to run a singular testsuite or all the suites in the directory
```
$ robot --outputdir artifacts/ robot/testsuites/integration/<UserScenario>
$ robot --outputdir artifacts/ robot/testsuites/integration/<UserScenario>/<testcase>.robot
```


### Generation of documentation

To generate Keywords documentation:
```
python3 -m robot.libdoc robot/resources/lib/neofs.py docs/NeoFS_Library.html
python3 -m robot.libdoc robot/resources/lib/payment_neogo.py docs/Payment_Library.html
```

To generate testcases documentation:
```
python3 -m robot.testdoc robot/testsuites/integration/ docs/testcases.html
```

### Source code overview

`robot/` - Files related/depended on Robot Framework.

`robot/resources/` - All resources (Robot Framework Keywords, Python Libraries, etc) which could be used for creating test suites.

`robot/resources/lib/` - Common Python Libraries depended on Robot Framework (with Keywords). For example neofs.py, payment.py.

`robot/variables/` - All variables for tests. It is possible to add the auto-loading logic of parameters from the smart-contract in the future. Contain python files.

`robot/testsuites/` - Robot TestSuites and TestCases.

`robot/testsuites/integration/` - Integration test suites and testcases

### Code style

Robot Framework keyword should use space as a separator between particular words

The name of the library function in Robot Framework keyword usage and the name of the same function in the Python library must be identical.

The name of GLOBAL VARIABLE must be in UPPER CASE, the underscore ('_')' symbol must be used as a separator between words.

The name of local variable must be in lower case, the underscore symbol must be used as a separator between words.

The names of Python variables, functions and classes must comply with accepted rules, in particular:
Name of variable/function must be in lower case with underscore symbol between words
Name of class must start with a capital letter. It is not allowed to use underscore symbol in name, use capital for each particular word.
For example: NeoFSConf

Name of other variables should not be ended with underscore symbol

On keywords definition, one should specify variable type, e.g. path: str

### Robot style

You should always complete the [Tags] and [Documentation] sections for Testcases and Documentation for Test Suites.

### Robot-framework User Guide

http://robotframework.org/robotframework/latest/RobotFrameworkUserGuide.html

## PyTest

Tests written with PyTest framework are located under `pytest_tests/testsuites` directory.

### Run and get report

1. Prepare virtualenv

```
$ make venv.local-pytest
$ . venv.local-pytest/bin/activate
```

2. Install Allure CLI

Allure CLI installation is not an easy task. You may select one of the following ways. If none of the options would help you please complete the instruction with your approach:

- Follow the [instruction](https://docs.qameta.io/allure/#_linux) from the official website
- Consult [the thread](https://github.com/allure-framework/allure2/issues/989)
- Download release from the Github
```
$ wget https://github.com/allure-framework/allure2/releases/download/2.18.1/allure_2.18.1-1_all.deb
$ sudo apt install ./allure_2.18.1-1_all.deb
```
You also need the `default-jre` package installed.

3. Run tests

In the activated virtualenv, execute the following command(s) to run a singular testsuite or all the suites in the directory
```
$ pytest --alluredir my-allure-123 pytest_tests/testsuites/object/test_object_api.py
$ pytest --alluredir my-allure-123 pytest_tests/testsuites/
```

4. Generate report

To generate a report, execute the command `allure generate`. The report will be under the `allure-report` directory.
```
$ allure generate my-allure-123
$ ls allure-report/
app.js  data  export  favicon.ico  history  index.html  plugins  styles.css  widgets
```

To inspect the report in a browser, run
```
$ allure serve my-allure-123
```
