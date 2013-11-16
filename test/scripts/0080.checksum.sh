start_test Checksum
test_data 1
mad md5 a001.test
grep -q 'md5' .a001.test.mad
cat .a001.test.mad