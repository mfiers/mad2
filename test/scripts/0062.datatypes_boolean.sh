start_test Data types - boolean
test_data 1

#
# boolean
#

mad set backup t a001.test
grep -q 'backup: true' .a001.test.mad
mad set backup y a001.test
grep -q 'backup: true' .a001.test.mad
mad set backup true a001.test
grep -q 'backup: true' .a001.test.mad
mad set backup yes a001.test
grep -q 'backup: true' .a001.test.mad

mad set backup f a001.test
grep -q 'backup: false' .a001.test.mad
mad set backup n a001.test
grep -q 'backup: false' .a001.test.mad
mad set backup false a001.test
grep -q 'backup: false' .a001.test.mad
mad set backup no a001.test
grep -q 'backup: false' .a001.test.mad

