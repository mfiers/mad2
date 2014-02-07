start_test Data types - string
test_data 1

mad set analyst Mark a001.test
mad set organism "Sus scrofa" a001.test

#
# date
#

mad set backup_until '3 march' a001.test
grep 'backup_until' .a001.test.mad | grep -q '03-03'

mad set backup_until '2015' a001.test
grep 'backup_until' .a001.test.mad | grep -q '2015'
