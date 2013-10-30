start_test Basic functionality

test_data 1

mad set analyst Mark a001.test
[[ -f ".a001.test.mad" ]] || false
grep -q 'analyst: Mark' .a001.test.mad

