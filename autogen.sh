#!/bin/sh

if [ -d .git ]; then
	git pull 
	git submodule init	
	https=`git config --get remote.origin.url | grep https`
	if [ "${https}" != "" ]
	then
			cur=`git config submodule.fl_libs.url | grep git@ | tr ":" "/"`
			if [ "${cur}" != "" ]
			then
					git config submodule.fl_libs.url https://${cur:4}
			fi
	fi
	git submodule update && git submodule status
	currentBranch=`git rev-parse --abbrev-ref HEAD`
	cd fl_libs && git checkout ${currentBranch}
	cd ..
fi
autoreconf --install
