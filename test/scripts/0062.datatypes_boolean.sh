start_test Data types - boolean
test_data 1

#
# boolean
#

mad set backup t a001.test
mad show a001.test | grep backup | grep -q True

mad set backup y a001.test
mad show a001.test | grep backup | grep -q True

mad set backup true a001.test
mad show a001.test | grep backup | grep -q True

mad set backup yes a001.test
mad show a001.test | grep backup | grep -q True

mad set backup f a001.test
mad show a001.test | grep backup | grep -q False

mad set backup n a001.test
mad show a001.test | grep backup | grep -q False

mad set backup false a001.test
mad show a001.test | grep backup | grep -q False

mad set backup no a001.test
mad show a001.test | grep backup | grep -q False



