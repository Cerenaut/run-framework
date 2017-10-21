#!/bin/bash

# e.g.  ./remote-run.sh 192.168.1.103 ~/.ssh/minsky incubator /Users/incubator/agief-project/variables/variables-minsky.sh

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
echo "Using port = " $port

ssh -v -p $port -i $keyfile ${user}@${host} -o 'StrictHostKeyChecking no' VARIABLES_FILE=$remote_variables_file 'bash --login -s' <<'ENDSSH' 
	export VARIABLES_FILE=$VARIABLES_FILE
	source $VARIABLES_FILE
	cd $AGI_HOME/bin/node_coordinator
	./run-in-docker.sh -d
ENDSSH

status=$?

if [ $status -ne 0 ]
then
	echo "ERROR: Could not complete execute run-in-docker.sh on remote machine through ssh." >&2
	echo "	Error status = $status" >&2
	echo "	Exiting now." >&2
	exit $status
fi


exit
ssh -i ~/.ssh/nextpair.pem ec2-user@52.63.242.158 "bash -c \"export VARIABLES_FILE=\"variables-ec2.sh\" && cd /home/ec2-user/agief-project/agi/bin/node_coordinator && ./run-in-docker.sh -d\""

ssh -i ~/.ssh/inc-box incubator@192.168.1.100 "bash -c \"ls\""