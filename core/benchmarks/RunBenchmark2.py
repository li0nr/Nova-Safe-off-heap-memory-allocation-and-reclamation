import subprocess
import time

Lists = ["N","U"]
Threads = ["1","16"]
for list in Lists:
        for thread in Threads:
            file1  = open("results"+list, "a")
        #p=os.system('java -cp ../nova-0.0.1-SNAPSHOT.jar -server -Xmx64g -XX:MaxDirectMemorySize=4g  com.yahoo.oak.SequntialBenchmark  N R 1 50000000&')
            args = ["java","-cp","../nova-0.0.1-SNAPSHOT.jar", "-XX:NativeMemoryTracking=summary","com.yahoo.oak.BenchmarkDelete",list,thread]
            print("testsing ",args)
            p = subprocess.run(args, stdout=subprocess.PIPE) # Success!
            time.sleep( 2 )
            file1.write(p.stdout.decode('ASCII'))
            print(p.stdout.decode('ASCII'))

Threads = ["2","4","8","16"]
Modes = ["R","W"]
for  l in Lists:
	for mode in Modes:
            for thread in Threads:
                file1  = open("results"+mode+list, "a")
            #p=os.system('java -cp ../nova-0.0.1-SNAPSHOT.jar -server -Xmx64g -XX:MaxDirectMemorySize=4g  com.yahoo.oak.SequntialBenchmark  N R 1 50000000&')
                args = ["java","-cp","../nova-0.0.1-SNAPSHOT.jar","-server", "-Xmx64g","-XX:MaxDirectMemorySize=4g", "com.yahoo.oak.BenchmarkConcurrent",l ,mode,thread]
                print("testsing ",args)
                p = subprocess.run(args, stdout=subprocess.PIPE) # Success!
                time.sleep( 2 )
                file1.write(p.stdout.decode('ASCII'))
                print(p.stdout.decode('ASCII'))
