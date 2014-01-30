start_test Data types - restricted
test_data 1

#
# restricted
#

#this is ok,
mad set status delete a001.test

#this should not be allowed
( if mad set status not_allowed a001.test;
    then false;
    else true; fi ) > /dev/null
