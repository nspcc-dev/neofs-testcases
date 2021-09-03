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

4. Install Testcases dependencies
    - `pip3.8 install -r requirements.txt`
    - `make deps`

Test cases are designed to run on Python 3.8.

### Run

0. Add keywords repo to PYTHONPATH `export PYTHONPATH=${PYTHONPATH}:~/neofs-keywords/lib::~/neofs-keywords/robot`

1. Clone neofs-dev-env and prepare it

```
# clean up obsolete volumes
make down
# restart
make clean
make up
# decrease maximum object size to 1000 bytes
make update.max_object_size val=1000
```
Also disable Storage Nodes Morph cache

```
export NEOFS_MORPH_DISABLE_CACHE=true
```

2. Export neofs-dev-env variables into the shell where you are going to run tests

```
cd neofs-dev-env
export `make env`
```

3. Execute the command `make run`

4. Logs will be available in the artifacts/ directory after tests with any of the statuses are completed.


### Running an arbitrary test case

To run an arbitrary UserScenario or testcase, you need to run the command:
`robot --outputdir artifacts/ robot/testsuites/integration/<UserScenario>` or `robot --outputdir artifacts/ robot/testsuites/integration/<UserScenario>/<testcase>.robot`

The following UserScenarios and testcases are available for execution:

 * acl
     * acl_basic_private_container_storagegroup.robot
     * acl_basic_private_container.robot
     * acl_basic_public_container_storagegroup.robot
     * acl_basic_public_container.robot
     * acl_basic_readonly_container_storagegroup.robot
     * acl_basic_readonly_container.robot
     * acl_bearer_allow_storagegroup.robot
     * acl_bearer_allow.robot
     * acl_bearer_compound.robot
     * acl_bearer_filter_oid_equal.robot
     * acl_bearer_filter_oid_not_equal.robot
     * acl_bearer_filter_userheader_equal.robot
     * acl_bearer_filter_userheader_not_equal.robot
     * acl_bearer_inaccessible.robot
     * acl_bearer_request_filter_xheader_deny.robot
     * acl_bearer_request_filter_xheader_equal.robot
     * acl_bearer_request_filter_xheader_not_equal.robot
     * acl_extended_actions_other.robot
     * acl_extended_actions_pubkey.robot
     * acl_extended_actions_system.robot
     * acl_extended_actions_user.robot
     * acl_extended_compound.robot
     * acl_extended_filters.robot
 * network
     * netmap_simple.robot
     * replication.robot
 * object
     * object_complex.robot
     * object_simple.robot
     * object_storagegroup_simple.robot
     * object_storagegroup_complex.robot
     * object_expiration.robot
 * payment
     * withdraw.robot
 * services
     * http_gate.robot
     * s3_gate.robot


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
