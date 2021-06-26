#!/usr/bin/env bash
dir=`pwd`
output=${dir}/output
java=java
jarfile="target/nova-synchrobench-0.0.1-SNAPSHOT.jar"

thread="01 02 04 06 08 "
size="20000"
keysize="4"
valuesize="4"
#writes="0 50"
writes="0"
warmup="30"
iterations="3"
duration="30000"
#gcAlgorithms="-XX:+UseParallelOldGC -XX:+UseConcMarkSweepGC -XX:+UseG1GC"

declare -A heap_limit=(
						["LL_EBR_CAS_bench"]="10m"
						["LL_EBR_noCAS_bench"]="10m"
						["LL_EBR_noCAS_opt_bench"]="10m"
						["LL_EBR_noCAS_opt_bench"]="10m"

						["LL_HE_CAS_bench"]="10m"
						["LL_HE_noCAS_bench"]="10m"
						["LL_HE_noCAS_opt_bench"]="10m"
						["LL_HE_noCAS_opt_bench"]="10m"

                       ["LL_Nova_CAS_bench"]="10m"
                       ["LL_Nova_noCAS_bench"]="10m"
                       ["LL_Nova_primitive_CAS_bench"]="10m"
                       ["LL_Nova_primitive_noCAS_bench"]="10m"
						
                       ["LL_NoMM_Synch"]="10m"
                       ["LL_Synch"]="30m"
                      )

declare -A direct_limit=(						
						["LL_EBR_CAS_bench"]="20m"
						["LL_EBR_noCAS_bench"]="20m"
						["LL_EBR_noCAS_opt_bench"]="20m"
						["LL_EBR_noCAS_opt_bench"]="20m"

						["LL_HE_CAS_bench"]="20m"
						["LL_HE_noCAS_bench"]="20m"
						["LL_HE_noCAS_opt_bench"]="20m"
						["LL_HE_noCAS_opt_bench"]="20m"

                       ["LL_Nova_CAS_bench"]="20m"
                       ["LL_Nova_noCAS_bench"]="20m"
                       ["LL_Nova_primitive_CAS_bench"]="20m"
                       ["LL_Nova_primitive_noCAS_bench"]="20m"
						
                       ["LL_NoMM_Synch"]="20m"
                       ["LL_Synch"]="0"
                        )

declare -A becnh_size=(						
						["LL_EBR_CAS_bench"]="10000"
						["LL_EBR_noCAS_bench"]="10000"
						["LL_EBR_noCAS_opt_bench"]="10000"
						["LL_EBR_noCAS_opt_bench"]="10000"

						["LL_HE_CAS_bench"]="10000"
						["LL_HE_noCAS_bench"]="10000"
						["LL_HE_noCAS_opt_bench"]="10000"
						["LL_HE_noCAS_opt_bench"]="10000"

                       ["LL_Nova_CAS_bench"]="10000"
                       ["LL_Nova_noCAS_bench"]="10000"
                       ["LL_Nova_primitive_CAS_bench"]="10000"
                       ["LL_Nova_primitive_noCAS_bench"]="10000"
						
                       ["LL_NoMM_Synch"]="10000"
                       ["LL_Synch"]="10000"
					   )
						
declare -A key_size=(
						["LL_EBR_CAS_bench"]="100"
						["LL_EBR_noCAS_bench"]="100"
						["LL_EBR_noCAS_opt_bench"]="100"
						["LL_EBR_noCAS_opt_bench"]="100"

						["LL_HE_CAS_bench"]="100"
						["LL_HE_noCAS_bench"]="100"
						["LL_HE_noCAS_opt_bench"]="100"
						["LL_HE_noCAS_opt_bench"]="100"

                       ["LL_Nova_CAS_bench"]="100"
                       ["LL_Nova_noCAS_bench"]="100"
                       ["LL_Nova_primitive_CAS_bench"]="100"
                       ["LL_Nova_primitive_noCAS_bench"]="100"
						
                       ["LL_NoMM_Synch"]="100"
                       ["LL_Synch"]="100"
					   
					   )

declare -A val_size=(						["LL_EBR_CAS_bench"]="1024"
						["LL_EBR_noCAS_bench"]="1024"
						["LL_EBR_noCAS_opt_bench"]="1024"
						["LL_EBR_noCAS_opt_bench"]="1024"

						["LL_HE_CAS_bench"]="1024"
						["LL_HE_noCAS_bench"]="1024"
						["LL_HE_noCAS_opt_bench"]="1024"
						["LL_HE_noCAS_opt_bench"]="1024"

                       ["LL_Nova_CAS_bench"]="1024"
                       ["LL_Nova_noCAS_bench"]="1024"
                       ["LL_Nova_primitive_CAS_bench"]="1024"
                       ["LL_Nova_primitive_noCAS_bench"]="1024"
						
                       ["LL_NoMM_Synch"]="1024"
                       ["LL_Synch"]="1024"
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


# Oak vs JavaSkipList
benchClassPrefix="com.yahoo.oak"
benchs="LL_EBR_CAS_bench  LL_EBR_noCAS_bench LL_EBR_noCAS_opt_bench LL_EBR_noCAS_opt_bench 
LL_HE_CAS_bench LL_HE_noCAS_bench LL_HE_noCAS_opt_bench LL_HE_noCAS_opt_bench 
LL_Nova_CAS_bench LL_Nova_noCAS_bench LL_Nova_primitive_CAS_bench LL_Nova_primitive_noCAS_bench 
LL_NoMM_Synch LL_Synch"

summary="${output}/summary.csv"

echo "Starting nova test `date`"
echo "Scenario, Bench, Heap size, Direct Mem, # Threads, Throughput, stdError" > ${summary}

for scenario in ${!scenarios[@]}; do
 for bench in ${benchs}; do
    echo ""
    echo "Scenario: ${bench} ${scenario}"
    heapSize="${heap_limit[${bench}]}"
    directMemSize="${direct_limit[${bench}]}"
    benchSize="${becnh_size[${bench}]}"
	keySize="${key_size[${bench}]}"
	valSize="${val_size[${bench}]}"
    for heapLimit in ${heapSize}; do
      #for gcAlg in ${gcAlgorithms}; do
        gcAlg=""
        javaopt="-server -Xmx${heapLimit} -XX:MaxDirectMemorySize=${directMemSize} ${gcAlg}"
        for write in ${writes}; do
          for t in ${thread}; do
            #for i in ${size}; do
              r=`echo "2*${benchSize}" | bc`
              out=${output}/oak-${scenario}-${bench}-xmx${heapLimit}-DirectMeM${directMemSize}-t${t}-${gcAlg}.log
              cmd="${java} ${javaopt} -jar ${jarfile} -b ${benchClassPrefix}.${bench} ${scenarios[$scenario]} -k ${keySize} -v ${valSize} -i ${benchSize} -r ${r} -n ${iterations} -t ${t} -d ${duration} -W ${warmup}"
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
done
echo "Oak test complete `date`"
