#!/usr/bin/env bash
dir=`pwd`
output=${dir}/output
java=java
jarfile="target/nova-synchrobench-0.0.1-SNAPSHOT.jar"

thread="01 04 08 12 16 20 24 28 32"
keysize="512"
valuesize="1024"
warmup="30"
iterations="3"
duration="30000"

						
                        
                        
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
#                      ["25del-25put-50search"]="-a 25 -u 50 -s 90"
#                      ["05del-05put-90search"]="-a 5 -u 10 -s 90"
                     )


benchClassPrefix="com.yahoo.oak"


benchs="LL_EBR_noCAS_bench
		LL_EBR_noCAS_opt_bench  
		LL_HE_noCAS_bench 
		LL_HE_noCAS_opt_bench		
		LL_Nova_noCAS_bench 
		LL_Nova_primitive_noCAS_bench 
		LL_Nova_Magic_noCAS_bench
		LL_NoMM_Synch LL_GC"

summary="${output}/summary.csv"

echo "Starting nova test `date`"
echo "Scenario, Bench, Heap size, Direct Mem, # Threads, Throughput, stdError" > ${summary}

for scenario in ${!scenarios[@]}; do
 for bench in ${benchs}; do
    echo ""
    echo "Scenario: ${bench} ${scenario}"
    benchSize=65536
    javaopt="-server "
	for t in ${thread}; do
		r=`echo "2*${benchSize}" | bc`
		out=${output}/nova-${scenario}-${bench}-t${t}.log
		cmd="${java} ${javaopt} -jar ${jarfile} -b ${benchClassPrefix}.${bench} ${scenarios[$scenario]} -k ${keysize} -v ${valuesize} -i ${benchSize} -r ${r} -n ${iterations} -t ${t} -d ${duration} -W ${warmup}"
		echo ${cmd}
		echo ${cmd} >> ${out}
		${cmd} >> ${out} 2>&1
		# update summary
		finalSize=`grep "Mean Total Size:" ${out} | cut -d : -f2 | tr -d '[:space:]'`
		throughput=`grep "Mean:" ${out} | cut -d : -f2 | tr -d '[:space:]'`	    
		std=`grep "Standard error:" ${out} | cut -d : -f2 | tr -d '[:space:]'`
		echo "${scenario}, ${bench}, ${heapLimit}, ${directMemSize}, ${t},${throughput},${std}" >> ${summary}
		done
      echo "" >> ${summary}
    done
  done
echo "Nova test complete `date`"
