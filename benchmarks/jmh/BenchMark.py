import subprocess
import os
import time

List = ["Un", "Nova", "HE", "NoMM"]
threads = ["1", "2", "4", "8"]#, "12", "16"]#,"20","24","28","32"]
modes = ["Seq", "Rand"]
operation = ["Write", "Read"]


# modes = []#"R","W","R_Rand","W_Rand","concurrent"]

# "-server", "-Xmx64g", "-XX:MaxDirectMemorySize=4g",
def Bench():
    for mod in modes:
        for op in operation:
            csvfile = open(mod + op + ".csv", "a")
            for l in List:
                csvfile.write(l+"\n")
                file1 = open("_" + l + mod + op + ".txt", "a")
                for t in threads:
                    args = ["java", "-Xmx64g", "-server", "-jar", "target/benchmarks.jar", "List" + l + mod+"."+op,
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
