import subprocess
import os
import time

print(__name__)
os.chdir(os.path.abspath(os.path.expanduser('../target/classes')))
Lists = ["N"]
Modes = ["R","W"]
Threads = ["1","2","4" ,"8","12","16"]#,"20","24","28","32"]

for list in Lists:
	for mode in Modes:
		file1  = open("results"+mode+list, "a")
		for thread in Threads:
			args = ["java","-server", "-Xmx64g","-XX:MaxDirectMemorySize=4g", "com.yahoo.oak.SequntialBenchmark",list,mode,thread,"100"]
			print("testsing ",args)
			p = subprocess.run(args, stdout=subprocess.PIPE) # Success!
			time.sleep( 5 )
			file1.write(p.stdout.decode('ASCII'))
			print(p.stdout.decode('ASCII'))
		print("changeing modes")
		file1.close()
	print("changeing lists")
	file1.close()

for list in Lists:
	for mode in Modes:
		file1  = open("results"+mode+list+"rand", "a")
		for thread in Threads:
			args = ["java","-server", "-Xmx64g","-XX:MaxDirectMemorySize=4g", "com.yahoo.oak.RandomBenchmark",list,mode,thread,"100"]
			print("testsing ",args)
			p = subprocess.run(args, stdout=subprocess.PIPE) # Success!
			time.sleep( 5 )
			file1.write(p.stdout.decode('ASCII'))
			print(p.stdout.decode('ASCII'))
		print("changeing modes")
		file1.close()
	print("changeing lists")
	file1.close()