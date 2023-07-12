# !/bin/bash
# This script create a release in ByConity github project
version=$1
location=$2
gh release create $version $location/byconity*

