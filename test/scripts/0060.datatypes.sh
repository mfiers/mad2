start_test Data types - string
test_data 1
mad set analyst Mark a001.test
mad set organism arabidopsis a001.test

#
# date
#

mad set backup_until '3 march' a001.test 2>/dev/null
grep 'backup_until' .a001.test.mad | grep -q '03-03'

mad set backup_until '2014' a001.test 2>/dev/null
grep 'backup_until' .a001.test.mad | grep -q '2014'
