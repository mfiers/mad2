start_test Checksum
test_data 1
mad sha1 a001.test >/dev/null
grep 'sha1' .a001.test.mad | \
        grep -q 'd998709663ead3cc63c34d76213a19121ebe2dfb'

mad sha1 -w a001.test 2>&1 | \
        grep -q 'Skipping sha1'

echo 'x1231234x' >> a001.test
if mad sha1 -w a001.test 2>&1 | \
        grep -vq 'Skipping sha1'
then
    false;
fi

# #grep sha1 .a001.test.mad | grep -q '3461d6b12f38b05a445a4d81be1197e1e304f319'
