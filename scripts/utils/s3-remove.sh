#!/bin/bash

FILE_SIZE_LIMIT=100000000 # in bytes

BUCKET_NAME="agief-project"
FOLDER_PATH="experiment-output"

SEARCH_TERM="log4j2.log"

output="$(aws s3 ls s3://$BUCKET_NAME/$FOLDER_PATH --recursive | grep $SEARCH_TERM)"

files_to_delete=()

IFS='
'

# Iterate over list of files to extract filename and size
for x in $output
do
  file_size="$(echo $x | awk -v col=3 '{print $col}')"
  file_name="$(echo $x | awk -v col=4 '{print $col}')"
 
  # Add large files to deletion array
  if [ "$file_size" -ge "$FILE_SIZE_LIMIT" ]
  then
    files_to_delete+=($file_name)
  fi
done

# Delete selected files
for file in "${files_to_delete[@]}"
do
  cmd="aws s3 rm s3://$BUCKET_NAME/$file"
  echo $cmd
  eval $cmd
done

