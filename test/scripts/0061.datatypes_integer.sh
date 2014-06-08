start_test Data types - integer
test_data 1

#
# integer
#
mad set temp_keep 3 a001.test

mad show a001.test \
    | grep temp_keep \
    | grep '3'
