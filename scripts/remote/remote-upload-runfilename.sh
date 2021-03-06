#!/bin/bash


default="$(dirname $0)/../variables.sh"
variables_file=${VARIABLES_FILE:-$default}
echo "Using variables file = $variables_file" 
source $variables_file

if [ "$1" == "-h" -o "$1" == "--help" -o "$1" == "" ]; then
  echo "Usage: `basename $0` HOST KEY_FILE (default = ~/.ssh/ecs-key.pem)"
  exit 0
fi

# upload a file specified by 2nd parameter 'filename', located in the $AGI_RUN_HOME folder, to s3, in the experiment for prefix specified by 1st parameter

prefix=$1
filename=$2
host=$3
keyfile=${4:-$HOME/.ssh/ecs-key.pem}
user=${5:-ec2-user}
remote_variables_file=${6:-/home/ec2-user/agief-project/variables/variables-ec2.sh}
port=${7:-22}

echo "Using prefix = " $prefix
echo "Using filename = " $filename
echo "Using host = " $host
echo "Using keyfile = " $keyfile
echo "Using user = " $user
echo "Using remote_variables_file = " $remote_variables_file
echo "Using port = " $port

ssh -v -p $port -i $keyfile ${user}@${host} -o 'StrictHostKeyChecking no' prefix=$prefix filename=$filename VARIABLES_FILE=$remote_variables_file 'bash --login -s' <<'ENDSSH' 
	export VARIABLES_FILE=$VARIABLES_FILE
	source $VARIABLES_FILE

	upload_file=$AGI_RUN_HOME/$filename
	echo "Calculated upload_file = " $upload_file

	if [ -f $upload_file ]; then
		cmd="aws s3 cp $upload_file s3://agief-project/experiment-output/$prefix/$filename"
		echo $cmd >> remote-upload-rfname-cmd.log
		eval $cmd >> remote-upload-rfname-stdout.log 2>> remote-upload-stderr.log
	else
	 	echo $upload_file >> remote-upload-rfname-cmd.log
	 	echo "File does not exist" >> remote-upload-rfname-cmd.log
	fi
ENDSSH

status=$?

if [ $status -ne 0 ]
then
	echo "ERROR: Could not complete remote upload through ssh." >&2
	echo "	Error status = $status" >&2
	echo "	Exiting now." >&2
	exit $status
fi