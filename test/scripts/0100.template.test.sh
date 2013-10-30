start_test Basic template
test_data 9
mad = raw *.test
grep -q 'category: raw' .a009.test.mad
mad = intermediate *.test
#should overwrite!
grep -q 'category: intermediate' .a009.test.mad

