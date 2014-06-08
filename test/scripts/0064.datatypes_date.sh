start_test Data types - date
test_data 1

#
# date
#

mad set backup_until '3 march' a001.test
mad show a001.test \
    | grep 'backup_until' \
    | grep -q '....-..-.. ..:..:..'


mad set backup_until '2018' a001.test
mad show a001.test \
    | grep 'backup_until' \
    | grep -q 2018
