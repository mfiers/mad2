start_test Data types - date
test_data 1

#
# date
#

mad set backup_until '3 march' a001.test 2>/dev/null
grep 'backup_until' .a001.test.mad | grep -q '03-03'

mad set backup_until '2014' a001.test 2>/dev/null
grep 'backup_until' .a001.test.mad | grep -q '2014'
