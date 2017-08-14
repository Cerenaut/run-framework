#!/bin/bash

#This script is meant for making result extraction easier from the console output.
#It expects a file containing the console content of a successful experiment run,
#and removes everything but the lines directly related to the results.



#cutting the console output file from the begining up to the start of phase2 and
#throwing it away

FST_PFX="$(sed -n <"$1" '/^1 : prefix.*/=')"
echo "$FST_PFX"
CUTOFF=$(($FST_PFX-1))
echo "$CUTOFF"
sed -i  1,"$CUTOFF"d  "$1"

#filtering all unnecessary console noise.
sed  -i "s/^\(------\|----\|\.\|Experiment\|utils\|ECDSA\|Folder\|Git\|Variables\|launching\|Docker\|Try\|     Input\|      Data\|   ->\|Reporting\|        Entities\|        Data\|remote\|Std\|        docker\|        exit\|    incubator\|incubator\|  --- uploading\|logout\| \.\.\. file\|debug1\|Authenticated\|Transferred\|Bytes\| --- Upload\|mv: cannot\|  - framework\|WARNING\|Email\|Sending\|Finished\|(\|[a-z0-9]\{64\}\).*$//" "$1"

#compacting the lines
sed -i "/^$/d" "$1"
