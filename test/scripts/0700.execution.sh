if mad has_command x; then
	start_test Execution 1
	test_data 1
	mad x 'ls {{filename}} | sed "s/a00/b11/"' a001.test | grep -q "b111.test"
else
	skip_test
fi