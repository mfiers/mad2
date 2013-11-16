start_test Data types - lists
test_data 1

mad set xref test1 a001.test
mad set xref test1 a001.test
mad set +xref test1 a001.test
mad set xref test2 a001.test

[ $(grep 'test1' .a001.test.mad | wc -l) == 3 ]
[ $(grep 'test2' .a001.test.mad | wc -l) == 1 ]

#do not want to see '+xref'mad set
if grep -F '+xref' .a001.test.mad; then false; else true; fi

mad set category raw a001.test
#category does not allow lists - so this should fail:
(if mad set +category raw a001.test; then false; else true; fi) > /dev/null

