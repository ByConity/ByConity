# !/bin/bash
# This script create a release in ByConity github project
version=$1
gh release create $version deb/test_output/byconity*

