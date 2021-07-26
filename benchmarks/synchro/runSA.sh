#!/usr/bin/env bash
dir=`pwd`
output=${dir}/outputSA
java=java
jarfile="target/nova-synchrobench-0.0.1-SNAPSHOT.jar"

thread="01 04 08 12 16 20 24 28 32"
size="20000"
keysize="1024"
#writes="0 50"
writes="0"
warmup="30"
iterations="3"
duration="15000"
#gcAlgorithms="-XX:+UseParallelOldGC -XX:+UseConcMarkSweepGC -XX:+UseG1GC"

declare -A heap_limit=(
						["SA_EBR","05del-05put-90search"]="110m"
						["SA_EBR","25del-25put-50search"]="110m"
						["SA_EBR","50del-50put"]="110m"

						["SA_HE","05del-05put-90search"]="110m"
						["SA_HE","25del-25put-50search"]="110m"
						["SA_HE","50del-50put"]="110m"

						["SA_Nova","05del-05put-90search"]="110m"
						["SA_Nova","25del-25put-50search"]="110m"
						["SA_Nova","50del-50put"]="110m"
						
						
						["SA_Nova_primitive","05del-05put-90search"]="110m"
						["SA_Nova_primitive","25del-25put-50search"]="110m"
						["SA_Nova_primitive","50del-50put"]="110m"

						["SA_NovaFenceFree","05del-05put-90search"]="110m"
						["SA_NovaFenceFree","25del-25put-50search"]="110m"	
						["SA_NovaFenceFree","50del-50put"]="110m"
						
						
						["SA_NoMM","05del-05put-90search"]="110m"
						["SA_NoMM","25del-25put-50search"]="110m"
						["SA_NoMM","50del-50put"]="110m"

						["SA_GC","05del-05put-90search"]="1371m"
						["SA_GC","25del-25put-50search"]="1371m"
						["SA_GC","50del-50put"]="1371m"

						
                      )


declare -A becnh_size=(						
						["SA_EBR"]="1000000"
						["SA_HE"]="1000000"
						["SA_Nova"]="1000000"
						["SA_Nova_primitive"]="1000000"
						["SA_NovaFenceFree"]="1000000"
						["SA_NoMM"]="1000000"
						["SA_GC"]="1000000"
					   )
						                        
                        
if [ ! -d "${output}" ]; then
  mkdir $output
else
  rm -rf ${output}/*
fi


###############################
# records all benchmark outputs
###############################

declare -A scenarios=(
                      ["50del-50put"]="-a 50 -u 100"
                      ["25del-25put-50search"]="-a 25 -u 50 -s 90"
                      ["05del-05put-90search"]="-a 5 -u 10 -s 90"
                     )


benchClassPrefix="com.yahoo.oak"

benchs="SA_EBR SA_HE SA_Nova SA_Nova_primitive SA_Nova_FenceFree SA_NoMM SA_GC"

summary="${output}/summarySA.csv"

echo "Starting nova test `date`"
echo "Scenario, Bench, Heap size, Direct Mem, # Threads, Throughput, stdError" > ${summary}

for scenario in ${!scenarios[@]}; do
 for bench in ${benchs}; do
    echo ""
    echo "Scenario: ${bench} ${scenario}"
    heapSize="${heap_limit[${bench},${scenario}]}"
	echo ${heapSize}
	echo 
    directMemSize="${direct_limit[${bench}]}"
    benchSize="${becnh_size[${bench}]}"

    for heapLimit in ${heapSize}; do
      #for gcAlg in ${gcAlgorithms}; do
        gcAlg=""
        javaopt="-server -Xmx${heapLimit} -Xms${heapLimit} ${gcAlg}"
        for write in ${writes}; do
          for t in ${thread}; do
            #for i in ${size}; do
              r=`echo "${benchSize}" | bc`
              out=${output}/oak-${scenario}-${bench}-xmx${heapLimit}-Xms${heapLimit}-t${t}-${gcAlg}.log
              cmd="${java} ${javaopt} -jar ${jarfile} -b ${benchClassPrefix}.${bench} ${scenarios[$scenario]} -k ${keysize} -i ${benchSize} -r ${r} -n ${iterations} -t ${t} -d ${duration} -W ${warmup}"
              echo ${cmd}
              echo ${cmd} >> ${out}
              ${cmd} >> ${out} 2>&1
              # update summary
              finalSize=`grep "Mean Total Size:" ${out} | cut -d : -f2 | tr -d '[:space:]'`
              throughput=`grep "Mean:" ${out} | cut -d : -f2 | tr -d '[:space:]'`	    
              std=`grep "Standard error:" ${out} | cut -d : -f2 | tr -d '[:space:]'`
              echo "${scenario}, ${bench}, ${heapLimit}, ${directMemSize}, ${t},${throughput},${std}" >> ${summary}
            done
          done
        done
      #done
      echo "" >> ${summary}
  done
done
echo "SA test complete `date`"
