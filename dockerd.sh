#!/bin/bash

dockerd &
sleep 60
export DOCKER_HOST=unix:///var/run/docker.sock
make hosts -B -C /robot/vendor/neofs-dev-env | grep -v make >> /etc/hosts
make rebuild -C /robot/vendor/neofs-dev-env
make up -C /robot/vendor/neofs-dev-env
sleep 60
robot --timestampoutputs --outputdir /artifacts/ /robot/testsuites/integration/*.robot 
