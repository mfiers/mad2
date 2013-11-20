start_test Checksum
test_data 1
mad sha1 a001.test >/dev/null
grep 'sha1' .a001.test.mad | grep -q 'd998709663ead3cc63c34d76213a19121ebe2dfb'
