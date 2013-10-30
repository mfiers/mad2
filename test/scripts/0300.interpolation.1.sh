if mad has_command config; then
	start_test Render variables
	test_data 9
	mad config testkey testval
	mad set -f interdummy blabloe a008.test
	mad set -f dummy 'interpolate {{testkey}} {{ interdummy }}' a008.test
	grep -q interpolate .a008.test.mad
	mad print dummy a008.test | grep -q "interpolate testval blabloe"
fi