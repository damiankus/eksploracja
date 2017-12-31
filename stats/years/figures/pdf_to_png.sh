#!/usr/bin/env bash

target_ext=$1
rm -r $target_ext
mkdir $target_ext


for loc_dir in 19*/
do
	echo "Creating $loc_dir"
	mkdir $target_ext/$loc_dir
done

for filename in 19*/*.pdf
do
	no_ext=(${filename//./ })
	echo $no_ext
	pdftoppm $filename $target_ext/${no_ext[0]} -$target_ext -y 275 -H 1200
done

