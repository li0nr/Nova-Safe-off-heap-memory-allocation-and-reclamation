#!/usr/bin/env bash
# trap ctrl-c and call ctrl_c()
trap ctrl_c INT
CONTINUE=1

function ctrl_c() {
  echo "#### User enter CTRL-C"
  CONTINUE=0
}

############################################################################
# All test scenarios and benchmarks
############################################################################
declare -A scenarios=(
  ["25Put25Delete50Get"]="-a 25 -u 50 -s 50"
  ["50Put50Delete00Get"]="-a 50 -u 100 -s 0"
  ["05Put05Delete90Get"]="-a 5 -u 10 -s 90"
  #["40Put50Get"]="-u 50 -s 100 -oW"


  
)

declare -A benchmarks=(
  #["skip-list_GC"]="SkipList_OnHeap"
  #["skip-list_ZGC"]="SkipList_OnHeap"

  #["offheap-list-key"]="SkipList_OffHeap"
  #["offheap-list-key-EBR"]="SkipList_OffHeap_EBR"
  #["offheap-list-key-HE"]="SkipList_OffHeap_HE"
  #["offheap-list-key-NoMM"]="SkipList_OffHeap_NoMM"
  #["offheap-list-Segment"]="SkipList_OffHeap_MemSeg_allocator"
  ["offheap-list-key-Nova-object"]="SkipList_OffHeap_object"
  #["offheap-list-key-Nova-magic"]="SkipList_OffHeap_Magic"
)



declare -A gc_cmd_args=(
  #["default"]=""
  ["Serial"]="-XX:+UseSerialGC"
  ["Parallel"]="-XX:+UseParallelGC"
  #["CMS"]="-XX:+UseParNewGC" deprecated
  ["g1"]="-XX:+UseG1GC" #default
  ["zgc"]="-XX:+UseZGC"
  ["Shenandoah"]="-XX:+UseShenandoahGC"
  #["Epilon"]="-XX:+UnlockExperimentalVMOptions -XX:+UseEpsilonGC" does no reclamation https://openjdk.java.net/jeps/318
)

declare -A java_modes=(
  # JVM is launched in client mode by default in SUN/Oracle JDK.
  ["default"]=""

  # The Server VM has been specially tuned to maximize peak operating speed. It is intended for executing long-running
  # server applications, which need the fastest possible operating speed more than a fast start-up time or smaller
  # runtime memory footprint.
  ["server"]="-server"

  # The Java HotSpot Client VM has been specially tuned to reduce application start-up time and memory footprint, making
  # it particularly well suited for client environments. In general, the client system is better for GUIs.
  ["client"]="-client"
)



############################################################################
# Default arguments
############################################################################
java=java

# Default output is <pwd>/output.
output=$(pwd)/output

# Automatically picks the correct synchrobench JAR file
jar_file_path=$(find "$(pwd)" -name "nova-synchrobench-*.jar" | grep -v "javadoc" | xargs ls -1t | head -1)
# Pipes breakdown:
#  1. find the synchrobench JAR file (show full path)
#  2. exclude the JAR with the javadoc
#  3. sort by modification date (most recent first)
#  4. take the first result (the most recent JAR file)

# Iterate on the cartesian product of these arguments (space separated)
test_scenarios=${!scenarios[*]}
test_benchmarks=${!benchmarks[*]}
test_size=${!benchmark_Size[*]}
test_thread="1 4 8 12 16 20 24 28 32"
test_gc=${!gc_cmd_args[*]}
test_java_modes="server"

# Defines the key size
keysize="128"

# Defines the value size
valuesize="1024"

# Defines the number of warm-up (not measured) iterations.
warmup="30"

# Defines the number of measured iterations.
iterations="3"

# Defines the test runtime in milliseconds.
duration="30000"

# Defines the sampling range for queries and insertions.
range_ratio="2"

# This flag is used to debug the script before a long execution.
# If set to "1" (via "-v" flag in the command line), the script will produces all the output files (with all the runtime
# parameters), but will not run the benchmark command.
# Usage: when modifying the script for a specific test and running it for a long period (e.g., overnight),
# it is recommended to verify that all the parameters are as intended, and that the scripts will not fail for some
# reason after a few iterations only to be discovered in the morning.
verify_script=0

benchClassPrefix="com.yahoo.oak"

############################################################################
# Override default arguments
############################################################################
while getopts o:j:d:i:w:s:t:e:h:b:g:m:l:r:v opt; do
  case ${opt} in
  o) output=$OPTARG ;;
  j) java=$OPTARG ;;
  d) duration=$((OPTARG * 1000)) ;;
  i) iterations=$OPTARG ;;
  w) warmup=$OPTARG ;;
  h)
    for bench in ${!heap_limit[*]}; do
      heap_limit[${bench}]=$OPTARG
    done
    ;;
  l)
    for bench in ${!direct_limit[*]}; do
      direct_limit[${bench}]=$OPTARG
    done
    ;;
  r) range_ratio=$OPTARG ;;
  s) test_size=$OPTARG ;;
  t) test_thread=$OPTARG ;;
  e) test_scenarios=$OPTARG ;;
  b) test_benchmarks=$OPTARG ;;
  g) test_gc=$OPTARG ;;
  m) test_java_modes=$OPTARG ;;
  v) verify_script=1 ;;
  \?)
    echo "Invalid Option: -$OPTARG" 1>&2
    exit 1
    ;;
  :)
    echo "Invalid Option: -$OPTARG requires an argument" 1>&2
    exit 1
    ;;
  esac
done


############################################################################
# Changing working directory to the JAR file directory
############################################################################
synchrobench_path=$(dirname "${jar_file_path}")
jar_file_name=$(basename "${jar_file_path}")
echo "Found synchrobench JAR in: ${synchrobench_path}"
echo "Using JAR: ${jar_file_name}"
cd "${synchrobench_path}" || exit 1

############################################################################
# Initialized output folder
############################################################################
if [[ ! -d "${output:?}" ]]; then
  mkdir -p "${output}"
fi

timestamp=$(date '+%d-%m-%Y--%H-%M-%S')
summary="${output}/summary-${timestamp}.csv"

echo "Timestamp, Log File, Scenario, Bench, Heap size, Direct Mem, # Threads, GC, Final Size, Throughput, std" >"${summary}"

echo "Starting benchmark: $(date)"

# Iterate over a cartesian product of the arguments
for scenario in ${test_scenarios[*]}; do
	for bench in ${test_benchmarks[*]}; do
  echo ""
  echo "Scenario: ${bench} ${scenario}"
  echo "" >>"${summary}"

  scenario_args=${scenarios[${scenario}]}
  classPath="${benchClassPrefix}.${benchmarks[${bench}]}"
  
  gc_args="--add-modules jdk.incubator.foreign --add-opens jdk.incubator.foreign/jdk.internal.foreign=ALL-UNNAMED           --add-opens java.base/java.nio=ALL-UNNAMED --enable-native-access=ALL-UNNAMED"
  if [[ "$bench" == "skip-list_ZGC" ]]; then
	gc_args="-XX:+UseZGC"
	fi

	
    for java_mode in ${test_java_modes[*]};
	do  for thread in ${test_thread[*]}; do
        # Check if the user hit CTRL+C before we start a new iteration
        if [[ "$CONTINUE" -ne 1 ]]; then
          echo "#### Quiting..."
          exit 1
        fi
		benchSize="10000000"
		
		javaHeap=""
		javaOffHeap=""
		
		if [ "$bench" == "skip-list_GC" ] || [ "$bench" == "skip-list_ZGC" ]; then 
			javaHeap="-Xmx14G"
		else
			javaHeap="-Xmx4G"
			javaOffHeap="-o 10 -XX:MaxDirectMemorySize10G"

		fi
		
		java_args="${java_modes[${java_mode}]} ${gc_args} ${javaHeap}"

        # Set the range to a factor of the size of the data
        range=$((range_ratio * benchSize))

        # Add a timestamp prefix to the log file.
        # This allows repeating the benchmark with the same parameters in the future without removing the old log.
        timestamp=$(date '+%d-%m-%Y--%H-%M-%S')
        log_filename=${timestamp}-${scenario}-${bench}-size_${size}-t${thread}-m${java_mode}-gc${gc_alg}.log
        out=${output}/${log_filename}

        # Construct the command line as a multi-lined list for aesthetics reasons
        cmd_args=(
          "${java} ${java_args} -jar ${jar_file_name} -b ${classPath} ${scenario_args}"
          "-k ${keysize} -v ${valuesize} -i ${benchSize} -r ${range} -t ${thread}"
          "-W ${warmup} -n ${iterations} -d ${duration} ${javaOffHeap}"
        )
        cmd=${cmd_args[*]}
        echo "${cmd}"

        # Print all arguments to the log file
        {
          echo "[Arguments]"
          # General arguments:
          echo "timestamp: ${timestamp}"
          echo "log_filename: ${log_filename}"
          echo "synchrobench_path: ${synchrobench_path}"
          # Iteration arguments:
          echo "scenario: ${scenario}"
          echo "bench: ${bench}"
          echo "heap_limit: ${heapSize}"
          echo "direct_limit: ${directSize}"
          echo "gc_alg: ${gc_alg}"
          echo "gc_args: ${gc_args}"
          echo "java_mode: ${java_mode}"
          echo "write: ${write}"
          # CMD arguments:
          echo "cmd: ${cmd}"
          echo "java: ${java}"
          echo "java_args: ${java_args}"
          echo "jar_file_name: ${jar_file_name}"
          echo "classPath: ${classPath}"
          echo "scenario_args: ${scenario_args}"
          echo "keysize: ${keysize}"
          echo "valuesize: ${valuesize}"
          echo "warmup: ${warmup}"
          echo "iterations: ${iterations}"
          echo "size: ${size}"
          echo "range: ${range}"
          echo "thread: ${thread}"
          echo "duration: ${duration}"
          echo ""
          # The benchmark output will be appended here
          echo "[Output]"
        } >"${out}"

        if [[ "$verify_script" -ne 1 ]]; then
          ${cmd} >>"${out}" 2>&1

          # Read statistics from the output log
          finalSize=$(grep "Mean Total Size:" "${out}" | cut -d : -f2 | tr -d '[:space:]')
          throughput=$(grep "Mean:" "${out}" | cut -d : -f2 | tr -d '[:space:]')
          std=$(grep "Standard deviation pop:" "${out}" | cut -d : -f2 | tr -d '[:space:]')
        fi

        # Update summary
        summary_line=("${timestamp}" "${log_filename}" "${scenario}" "${bench}" "${heapSize}" "${directSize}"
          "${thread}" "${gc_alg}" "${finalSize}" "${throughput}" "${std}")
        (
          # Define the separator to be a comma instead of a space
          # This only have effect in the context of these parenthesis
          IFS=,
          echo "${summary_line[*]}"
        ) >>"${summary}"
      done; 
    done; 
  done;
  done ;
 

echo "SkipList test complete $(date)"
