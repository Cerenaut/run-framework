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
remote_variables_file=${4:-/home/ec2-user/agief-project/variables/variables-ec2.sh}
port=${5:-22}

echo "Using host = " $host
echo "Using keyfile = " $keyfile
echo "Using user = " $user
echo "Using remote_variables_file = " $remote_variables_file
echo "Using port " = $port

########################################################
# synch code and run folder with ecs instance
########################################################

# code
cmd="rsync -ave 'ssh -p $port -i $keyfile -o \"StrictHostKeyChecking no\"' $AGI_HOME/ ${user}@${host}:~/agief-project/agi --exclude={\"*.git/*\",*/src/*}"
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

# the specific experiment folder
cmd="rsync -ave 'ssh -p $port -i $keyfile -o \"StrictHostKeyChecking no\"'
	-f \"- input/*\" -f \"- output/*\"
	-f \"+ *\"
	$AGI_EXP_HOME/ ${user}@${host}:~/agief-project/run --exclude={\"*.git/*\"}"
echo $cmd
eval $cmd
status=$?

if [ $status -ne 0 ]
then
	echo "ERROR:  Could not complete rsync operation - failed at 'Experiment-Definitions' stage. Exiting now." >&2
	echo "	Error status = $status" >&2
	echo "	Exiting now." >&2
	exit $status
fi

# the variables folder (with variables.sh files)
cmd="rsync -ave 'ssh -p $port -i $keyfile -o \"StrictHostKeyChecking no\"' $AGI_EXP_HOME/../variables/ ${user}@${host}:~/agief-project/variables --exclude={\"*.git/*\"}"
echo $cmd
eval $cmd
status=$?

if [ $status -ne 0 ]
then
	echo "ERROR: Could not complete rsync operation - failed at 'Variables' stage. Exiting now." >&2
	echo "	Error status = $status" >&2
	echo "	Exiting now." >&2
	exit $status
fi