## Testcases execution

### Initial preparation

1. Install neofs-cli 
    - `git clone git@github.com:nspcc-dev/neofs-node.git`
    - `cd neofs-node` 
    - `make`
    - `sudo cp bin/neofs-cli /usr/local/bin/neofs-cli` or add alias path to bin/neofs-cli

2. Install cdn-authmate
    - `git clone git@github.com:nspcc-dev/cdn-authmate.git`
    - `cd cdn-authmate`
    - `make build`
    - `sudo cp bin/cdn-authmate /usr/local/bin/cdn-authmate` or add alias path to bin/cdn-authmate

3. Install Testcases dependencies
    - `pip3 install robotframework`
    - `pip3 install pexpect`
    - `pip3 install requests`
    - `pip3 install boto3`

(replace pip3 with the appropriate python package manager on the system).

In this case, dev-env should be running with the tested environment.

### Run

1. Execute the command `make run`

2. Logs will be available in the artifacts/ directory after tests with any of the statuses are completed.


### Running an arbitrary test case

To run an arbitrary testcase, you need to run the command:
`robot --timestampoutputs --outputdir artifacts/ robot/testsuites/integration/<testsuite name>.robot `

The following scripts are available for execution:

 * acl_basic.robot  
 * acl_extended.robot 
 * acl_baearer.robot 
 * object_complex.robot  
 * object_simple.robot  
 * withdraw.robot  
 * netmap_simple.robot 
 * replication.robot  
 * http_gate.robot  
 * s3_gate.robot 


## Smoke-test execution

There is a suite with smoke tests for CDN gates `robot/testsuites/smoke/selectelcdn_smoke.robot`.

By default, keywords use variables from a file `robot/resources/lib/neofs_int_vars.py`.
```
robot --outputdir artifacts/ robot/testsuites/smoke/selectelcdn_smoke.robot
```

### Initial preparation

1. It requires separate variables, unlike the NeoFS suites, which run on
dev-env. In order for the keyword libraries to use them, you need to set the environment variable
```
export ROBOT_PROFILE=selectel_smoke
```

Dev-env is not needed. But you need to install neo-go.

2. Install neo-go
    - `git clone git@github.com:nspcc-dev/neo-go.git`
    - `cd neo-go` 
    - `make`
    - `sudo cp bin/neo-go /usr/local/bin/neo-go` or add alias path to bin/neo-go

3. To run an arbitrary testcase: `robot --timestampoutputs --outputdir artifacts/ robot/testsuites/smoke/selectelcdn_smoke.robot`


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

`robot/testsuites/fi/` - Fault Injection testsuites and testcases

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
