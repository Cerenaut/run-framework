#!/bin/bash

default="$(dirname $0)/../variables.sh"
variables_file=${VARIABLES_FILE:-$default}
echo "Using variables file = $variables_file" 
source $variables_file

if [ "$1" == "-h" -o "$1" == "--help" -o "$1" == "" ]; then
  echo "Usage: `basename $0` HOST KEY_FILE (default = ~/.ssh/ecs-key.pem)"
  exit 0
fi

host=$1
keyfile=${2:-$HOME/.ssh/ecs-key.pem}
user=${3:-ec2-user}
remote_variables_file=${4:-/home/ec2-user/agief-python/variables/variables-tf.sh}
port=${5:-22}

echo "Using host = " $host
echo "Using keyfile = " $keyfile
echo "Using user = " $user
echo "Using remote_variables_file = " $remote_variables_file
echo "Using port " = $port

################################################################################
# Sync agief-remote-run
################################################################################

# code
cmd="rsync -ave 'ssh -p $port -i $keyfile -o \"StrictHostKeyChecking no\"' --exclude='.git/' $AGI_CODE_HOME/ ${user}@${host}:~/agief-remote-run"
echo $cmd
eval $cmd
status=$?

if [ $status -ne 0 ]
then
	echo "ERROR:  Could not complete rsync operation - failed at 'Code' stage." >&2
	echo "	Error status = $status" >&2
	echo "	Exiting now." >&2
	exit $status
fi