#!/bin/bash

# Before each test execution dev-env should be build down and up.

xmls=''

if [ $# -eq 0 ]; then 
    test_list=$(find ./robot/testsuites -regex '.*robot')
else
    test_list=$(find $@ -regex '.*robot')
fi

echo Tests to execute: $test_list

for test in $test_list; do
    pushd $DEVENV_PATH
    if [[ $test =~ 's3_gate' || $test =~ 'http_gate' ]]; then
        sed -i -e '/coredns/d' .services
    else
        sed -i -e '/coredns/d' -e '/s3_gate/d' -e '/http_gate/d' .services
    fi
    make down
    make clean
    make up
    make update.max_object_size val=1000
    popd
    test_addr=`echo $test | sed "s/\//_/g" | sed "s/.robot//"`
    robot --outputdir artifacts/ --output ${test_addr}_output.xml --log ${test_addr}_log.html --report ${test_addr}_report.html $test
    xmls+=" ./artifacts/${test_addr}_output.xml"
    pushd $DEVENV_PATH
    echo 'coredns' >> .services
    if [ -z $(cat .services | grep 's3_gate') ]; then echo 's3_gate' >> .services; fi
    if [ -z $(cat .services | grep 'http_gate') ]; then echo 'http_gate' >> .services; fi
    popd
done

rebot ${xmls}

