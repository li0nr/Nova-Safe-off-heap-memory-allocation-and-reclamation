import subprocess
import os
import time

List = ["Un", "Nova", "HE"]
threads = ["1", "2", "4", "8", "12", "16"]#,"20","24","28","32"]
modes = ["Seq", "Rand"]
operation = ["Write", "Read"]


# modes = []#"R","W","R_Rand","W_Rand","concurrent"]

# "-server", "-Xmx64g", "-XX:MaxDirectMemorySize=4g",
def Bench():
    for mod in modes:
        for op in operation:
            index = operation.index(op)
            exclude = operation[len(operation)-1-index] #what is this ???
            csvfile = open("tResults" + mod + op + ".csv", "a")
            for l in List:
                csvfile.write(l+"\n")
                file1 = open("tRawresults" + l + mod + op + ".txt", "a")
                for t in threads:
                    args = ["java", "-Xmx64g", "-server", "-jar", "target/benchmarks.jar", "List" + l + mod,
                            "-e", exclude,
                            "-rf", "csv",
                            "-f", "2", "-t", t]
                    print("testsing ", args)
                    p = subprocess.run(args, stdout = file1)  # Success!
                    with open("jmh-result.csv") as result:
                        read = result.readline().strip("\n")
                        for line in result:
                            csvfile.write(line)


if __name__ == "__main__":
    Bench()
