start_test Samplesheet
test_data 7
mad samplesheet --id identifier --apply ../metadata.xlsx a*.test
grep age .a004.test.mad | grep -q 55
