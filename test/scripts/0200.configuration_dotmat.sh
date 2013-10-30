start_test Configuration using .mad

if mad has_command config; then
	test_data 9
	mad config testkey testval
	cat .mad | grep -q 'testkey: testval'
else
	skip_test
fi
