#!/bin/sh

for i in `docker images | grep robot | awk '{ print $3 }'`; do
    docker rmi -f $i
done
