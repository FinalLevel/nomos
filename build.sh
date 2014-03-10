#!/bin/sh


git pull && git submodule init && git submodule update && git submodule status
currentBranch=`git rev-parse --abbrev-ref HEAD`
cd fl_libs && git checkout ${currentBranch}
autoreconf --install


