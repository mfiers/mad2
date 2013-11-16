start_test Directory level configuration

test_data 1
mad set ape kool a001.test
mad set testkey testval .
grep -q 'testkey: testval' .mad/config/_root.config
mad show a001.test | grep -q testkey

mkdir subsub
cd subsub
test_data 1
mad show . | grep -q 'testkey'
cd ..
