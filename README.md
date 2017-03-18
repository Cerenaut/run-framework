# Running AGIEF (end to end)

This README describes the use of  ```/scripts/run-framework/run-framework.py```.
It is a python script that is used to run and interact with the framework covering aspects such as generating input files, launching infrastrcture on AWS, running those experiments (locally or on AWS) and exporting the output. 

Setup and installation are required:
- [Setup](#markdown-header-setup)
- [Installation Instructions](#markdown-header-intallation-instructions)

AGIEF is in a separate repository named 'agi'. The components of AGIEF have their own run scripts, with their own READMEs. They can be found under ```/bin``` in the relevant subfolder, in the agi code repo.

The necessary experiment files are in another repo named 'experiment definitions'.

The system is shown graphically [here](https://docs.google.com/drawings/d/1zBIRn2o5c29C8w1IUUh38syWOqL4EiedtEpPHkgxDko/edit).

Note: All scripts (here and in 'agi') utilise environmental variables defined in a 'variables' file. Every script begins by sourcing this file. ```/resources/variables-template.sh``` is an example with explanations of each variable. You can modify that file, or create your own instead. 
*IMPORTANT:* Then set the ENV variable ```VARIABLES_FILE``` to it using the full path.

## Setup Environment
- get the latest latest
	- [code](https://github.com/ProjectAGI/agi) and 
	- [experiment definitions](https://github.com/ProjectAGI/experiment-definitions)
- setup your python environment (instructions in the next section)
- ensure values in your variables.sh file are correct
- build the framework (```agi/bin/node_coordinator/build.sh```)


## Installation Instructions
- Install Python and Pip
- Install dependencies
```pip install -r REQUIREMENTS.txt```
- Install and configure AWS-CLI (used by python script) [guide](http://docs.aws.amazon.com/cli/latest/userguide/installing.html)


## Run the framework - generate experiment input files
```run-framework.py --step_gen_input NAME_OF_MAIN_CLASS```

This will place the input files in your experiment definitions folder, referred to by ```$AGI_RUN_HOME```


## Run the framework - experiments
Use ```run-framework.py```. All of the steps can be run separately, or all together, specified with command line switches. Running ```python run-framework.py --help``` will give you more detail and the optional flags. 

The experiments can be run locally or on AWS. 

The experiments are run from the run folder ```$AGI_RUN_HOME/[Experiment Name]```, the same folder as the input files. The folder structure and required files are seen in the folder ```$AGI_HOME/resources/run-example```. In particular, ```node.properties``` has essential properties for the java process, and ```/input``` has the data for a run, and ```experiment.json``` defines the parameter sweeps and links to these input files.

The steps are:

- [aws] run the ec2 instances (ecs and postgres)
- launch framework
- import input files from run-folder
- [aws] sync code folder (compiled), run-folder to ecs (run-folder has node.properties, log4j xml etc.), and dataset
- run experiment
- optionally export artefacts
- change parameters and repeat run experiment
- shutdown framework
- [aws] shutdown ec2 instances


## Examples

### generate input files
```sh
python run-framework.py --exps_file experiments.json --step_gen_input io.agi.framework.demo.mnist.DeepMNISTDemo
python run-framework.py --exps_file experiments-phase1.json --step_gen_input io.agi.framework.demo.papers.KSparseDemo
```

### aws ecs and aws postgres (don't export or upload results), shutdown instances afterwards
```sh
python run-framework.py --logging --step_aws --exps_file experiments.json --step_sync --step_agief --step_shutdown --instanceid i-06d6a791 --port 8491 --pg_instance i-b1d1bd33 --task_name mnist-spatial-task:8 --ec2_keypath /$HOME/.ssh/ecs-key.pem
```

### aws run in a docker container on a new ec2 instance specified by AMI ID, node mode, export and upload results, shutdown instances afterwards
```sh
python run-framework.py --logging --step_aws --exps_file experiments.json --step_sync --step_compute --step_shutdown --step_export --step_upload --amiid ami-17211d74 --ami_ram 12 --port 8491 --ec2_keypath ~/.ssh/nextpair.pem
```

### local agief and local postgres (don't export or upload results)
```sh
python run-framework.py --logging --exps_file experiments.json --step_compute --host localhost --port 8491 --pg_instance localhost
```

### local agief (running in node mode i.e. no postgres required), export the output files, upload them to S3
python run-framework.py --exps_file experiments.json --step_compute --step_export --step_upload --host localhost --port 8491

### just run framework, don't import/export or run experiment
python run-framework.py --step_compute --host localhost --port 8491