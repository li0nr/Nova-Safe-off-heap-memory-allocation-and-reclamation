import subprocess
import os
import time

SMRs = ["Nova", "Nova_primitive", "HE", "EBR", "GC", "NoMM"]
threads = ["1", "2", "4", "8","12","16"]#,"20","24","28","32"]
def runJMH():

    for thread in threads:
        for SMR in SMRs:
            csvfile = open("Results" + thread + SMR + ".csv", "a")
            args = ["java", "-server", "-jar", "target/benchmarks.jar",
                    "SA.*"+SMR,
                    "-rf", "csv",
                    "-f", "3", "-t", thread]
            print("testsing ", args)
            p = subprocess.run(args, stdout = file1)  # Success!
            with open("jmh-result.csv") as result:
                read = result.readline().strip("\n")
                for line in result:
                    csvfile.write(line)

if __name__ == "__main__":
    runJMH()
