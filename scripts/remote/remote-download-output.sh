#!/bin/bash


default="$(dirname $0)/../variables.sh"
variables_file=${VARIABLES_FILE:-$default}
echo "Using variables file = $variables_file" 
source $variables_file

if [ "$1" == "-h" -o "$1" == "--help" -o "$1" == "" ]; then
  echo "Usage: `basename $0` HOST KEY_FILE (default = ~/.ssh/ecs-key.pem)"
  exit 0
fi

prefix=$1
host=$2
keyfile=${3:-$HOME/.ssh/ecs-key.pem}
user=${4:-ec2-user}
remote_variables_file=${5:-/home/ec2-user/agief-project/variables/variables-ec2.sh}

echo "Using prefix = " $prefix
echo "Using host = " $host
echo "Using keyfile = " $keyfile
echo "Using user = " $user
echo "Using remote_variables_file = " $remote_variables_file

ssh -v -i $keyfile ${user}@${host} -o 'StrictHostKeyChecking no' prefix=$prefix VARIABLES_FILE=$remote_variables_file 'bash -s' <<'ENDSSH' 
	export VARIABLES_FILE=$VARIABLES_FILE
	source $VARIABLES_FILE

	download_folder=$AGI_RUN_HOME/output/$prefix
	echo "Calculated download-folder = " $download_folder

	cmd="aws s3 cp s3://agief-project/experiment-output/$prefix/output $download_folder --recursive"
	echo $cmd >> remote-download-cmd.log
	eval $cmd >> remote-download-stdout.log 2>> remote-download-stderr.log
ENDSSH

status=$?

if [ $status -ne 0 ]
then
	echo "ERROR: Could not complete remote download through ssh." >&2
	echo "	Error status = $status" >&2
	echo "	Exiting now." >&2
	exit $status
fi