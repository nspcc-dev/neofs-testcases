#!/bin/bash

dockerd &
sleep 60
export DOCKER_HOST=unix:///var/run/docker.sock
docker login registry.nspcc.ru -u ${REG_USR} -p ${REG_PWD}
make rebuild -C /robot/vendor/neofs-dev-env
make up -C /robot/vendor/neofs-dev-env
sleep 60
robot --timestampoutputs --outputdir /artifacts/ /robot/testsuites/integration/object_suite.robot 
