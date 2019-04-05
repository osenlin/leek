#!/usr/bin/env bash

HOME="$(cd "`dirname "$0"`"/; pwd)"
PROJECT="leek"
SUB_PROJECT=("fdata/csr" "fdata/cotr" "fdata/cctr" "fdata/ctdr" "fdata/dc" "adata/sbs" "adata/ssid" "adata/sbsqa" "adata/ssidqa" "tdata/batch_task")
TARGET="target"

	
package () {
	ROOT_PROJECT=$1
	SUB=$2
	mkdir -p $HOME/$TARGET/$SUB/lib
	find ./$ROOT_PROJECT -name "*.py" -print | zip $ROOT_PROJECT.zip -@; mv $ROOT_PROJECT.zip $HOME/$TARGET/$SUB/lib
	# copy script
	mkdir -p $HOME/$TARGET/$SUB/bin
	cp -r $HOME/$ROOT_PROJECT/$SUB/*.py $HOME/$TARGET/$SUB/bin
	# copy config
	mkdir -p $HOME/$TARGET/$SUB/conf
	cp -r $HOME/$ROOT_PROJECT/$SUB/conf/* $HOME/$TARGET/$SUB/conf
}

# zip files
rm -rf $HOME/$TARGET
for i in "${SUB_PROJECT[@]}"
do
	package $PROJECT $i
done
