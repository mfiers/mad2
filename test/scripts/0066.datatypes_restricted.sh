start_test Data types - restricted
test_data 1

#
# restricted
#

#this is ok,
mad set category raw a001.test

#this should not be allowed
( if mad set category not_allowed a001.test; then false; else true; fi ) > /dev/null
