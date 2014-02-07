start_test Render variables

test_data 1

mad set testkey testval .
mad set -f interdummy blabloe a001.test
mad set -f dummy 'interpolate {{testkey}} {{ interdummy }}' a001.test
grep -q interpolate .a001.test.mad
mad print dummy a001.test | grep -q "interpolate testval blabloe"

### two level interpolation

mad set -f va 'eine' a001.test
mad set -f vb '{{ va }} kleine' a001.test
mad set -f vc '{{ vb }} nachtmusik' a001.test
mad print vc a001.test | grep -q '^eine kleine nachtmusik$'

### use directory configuration

mad set va 'keine' .
mad unset va a001.test
mad print vc a001.test | grep -q '^keine kleine nachtmusik$'
