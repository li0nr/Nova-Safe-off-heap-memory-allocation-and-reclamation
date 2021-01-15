import subprocess
import time

Lists = ["N","U"]
Modes = ["R","W"]
Threads = ["1","2","4","8","12","16","20","24","28","32"]
for list in Lists:
	for mode in Modes:
            for thread in Threads:
                file1  = open("results"+mode+list, "a")
            #p=os.system('java -cp ../nova-0.0.1-SNAPSHOT.jar -server -Xmx64g -XX:MaxDirectMemorySize=4g  com.yahoo.oak.SequntialBenchmark  N R 1 50000000&')
                args = ["java","-cp","../nova-0.0.1-SNAPSHOT.jar","-server", "-Xmx64g","-XX:MaxDirectMemorySize=4g", "com.yahoo.oak.SequntialBenchmark",list,mode,thread,"50000000"]
                print("testsing ",args)
                p = subprocess.run(args, stdout=subprocess.PIPE) # Success!
                time.sleep( 2 )
                file1.write(p.stdout.decode('ASCII'))
                print(p.stdout.decode('ASCII'))
for list in Lists:
	for mode in Modes:
            for thread in Threads:
                file1  = open("results"+mode+list, "a")
            #p=os.system('java -cp ../nova-0.0.1-SNAPSHOT.jar -server -Xmx64g -XX:MaxDirectMemorySize=4g  com.yahoo.oak.SequntialBenchmark  N R 1 50000000&')
                args = ["java","-cp","../nova-0.0.1-SNAPSHOT.jar","-server", "-Xmx64g","-XX:MaxDirectMemorySize=4g", "com.yahoo.oak.RandomBenchmark",list,mode,thread,"50000000"]
                print("testsing ",args)
                p = subprocess.run(args, stdout=subprocess.PIPE) # Success!
                time.sleep( 2 )
                file1.write(p.stdout.decode('ASCII'))
                print(p.stdout.decode('ASCII'))