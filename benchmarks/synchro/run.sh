#!/usr/bin/env bash
dir=`pwd`
output=${dir}/output
java=java
jarfile="target/nova-synchrobench-0.0.1-SNAPSHOT.jar"

thread="01 02 04 06 08 "
size="10000"
keysize="4"
valuesize="4"
#writes="0 50"
writes="0"
warmup="30"
iterations="3"
duration="30000"
#gcAlgorithms="-XX:+UseParallelOldGC -XX:+UseConcMarkSweepGC -XX:+UseG1GC"

declare -A heap_limit=(["BST_HE_Synch"]="10g"
                       ["BST_NoMM_Synch"]="10g"
                       ["BST_Nova_Synch"]="10g"
                      )

declare -A direct_limit=(["BST_HE_Synch"]="22g"
                         ["BST_NoMM_Synch"]="22g"
                         ["BST_Nova_Synch"]="22g"
                        )

echo $output

echo "stars"
if [ ! -d "${output}" ]; then
  mkdir $output
else
  rm -rf ${output}/*
fi


###############################
# records all benchmark outputs
###############################
