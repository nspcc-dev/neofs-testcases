#!/usr/bin/python3

ROOT='../..'

RESOURCES="%s/resources/lib" % ROOT
# path from repo root is required for object put and get
# in case when test is run from root in docker
ABSOLUTE_FILE_PATH="/robot/testsuites/integration"
