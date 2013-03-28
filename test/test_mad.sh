#/bin/bash -el

#catch errors
function on_error {
    echo "Caught an error :("
    echo "in $0, line: $1"
    exit -1
}

trap 'on_error ${LINENO}' ERR

#create dummy data
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

function start_test {
    echo "###### Test: $*"
}

#basic functionality
start_test General
test_data 1
cd test
mad set analyst Mark a001.test
[[ -f "a001.test.mad" ]] || false
grep 'analyst: Mark' a001.test.mad > /dev/null
cd ..

start_test Samplesheet
test_data 7
cd test
mad samplesheet --id identifier --apply ../metadata.xlsx a*.test
grep age a004.test.mad | grep -q 55
cd ..

start_test .mad configuration
test_data 9
cd test
mad config testkey testval
cat .mad | grep -q 'testkey: testval'
cd ..

start_test Render variables
test_data 9
cd test
mad config testkey testval
mad set -f interdummy blabloe a008.test
mad set -f dummy 'interpolate {{testkey}} {{ interdummy }}' a008.test
grep -q interpolate a008.test.mad 
mad print dummy a008.test | grep -q "interpolate testval blabloe"
cd ..

start_test templates
test_data 9
cd test
mad = raw *.test
grep -q 'category: raw' a009.test.mad
mad = intermediate *.test
#should not overwrite!
grep -q 'category: raw' a009.test.mad
cd ..

set +v
echo "Success."


