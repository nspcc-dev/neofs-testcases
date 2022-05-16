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

6. Build virtual env

In the cloned neofs-testcases repo execute the following commands:

```
make venv.localtest
. venv.localtest/bin/activate
```

Test cases are designed to run on Python 3.9.


## Run

To run an arbitrary UserScenario or testcase, you need to run the command:
`robot --outputdir artifacts/ robot/testsuites/integration/<UserScenario>` or `robot --outputdir artifacts/ robot/testsuites/integration/<UserScenario>/<testcase>.robot`


## Generation of documentation

To generate Keywords documentation:
```
python3 -m robot.libdoc robot/resources/lib/neofs.py docs/NeoFS_Library.html
python3 -m robot.libdoc robot/resources/lib/payment_neogo.py docs/Payment_Library.html
```

To generate testcases documentation:
```
python3 -m robot.testdoc robot/testsuites/integration/ docs/testcases.html
```

## Testcases implementation

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
