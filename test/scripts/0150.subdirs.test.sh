start_test Basic template
test_data 9
mad sha1 *
mkdir subdir && cd subdir
test_data 4
cd ..
mad sha1 subdir/*
