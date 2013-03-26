#!/bin/bash

set -ev

#catch errors
function on_error {
    echo "Caught an error :("
    echo "in $0, line: $1"
    exit -1
}

trap 'on_error ${LINENO}' ERR

#create some dummy data
function test_data {
    rm -rf test || true
    mkdir test
    for x in $(seq -w 1 $1)
    do
	for y in $(seq -w 1 $x)
	do 
	    echo $x $y >> test/a00$x.test
	done
    done
}

#basic functionality
test_data 1
cd test
mad set analyst Mark a001.test
ls
cat a001.test.mad
grep 'analyst: Mark' a001.test.mad
cd ..

echo "basic samplesheet operation"
test_data 7
cd test
mad samplesheet --id identifier --apply ../metadata.xlsx a*.test
grep age a004.test.mad | grep 55
cd ..

echo "config"
test_data 9
cd test
mad config testkey testval
mad set dummy 'interpolate {{testkey}}' a008.test
grep interpolate a008.test.mad

mad show a008.test.mad
cd ..


set +v
echo "Success."


