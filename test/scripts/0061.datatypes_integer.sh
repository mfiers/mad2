start_test Data types - integer
test_data 1

#
# integer
#
mad set temp_keep 3 a001.test

grep -q 'keep: 3' .a001.test.mad
( if mad set temp_keep not_an_integer a001.test ; then false; else true; fi) 2> /dev/null
