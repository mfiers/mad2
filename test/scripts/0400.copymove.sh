start_test Copy Move

#copy a number of files
test_data 5
mad sha1 *
mkdir other
mad cp a00?.test other
[ -f other/.a001.test.mad ]

#copying a single file - changing it's name
mkdir more
mad cp a001.test more/blabla
[ -f more/.blabla.mad ]
