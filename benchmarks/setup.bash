#!/bin/bash
sudo apt-get update
curl  -O https://download.java.net/java/GA/jdk17.0.2/dfd4a8d0985749f896bed50d7138ee7f/8/GPL/openjdk-17.0.2_linux-x64_bin.tar.gz
curl  -k -O https://dlcdn.apache.org/maven/maven-3/3.8.4/binaries/apache-maven-3.8.4-bin.tar.gz
sudo tar -xvf openjdk-17.0.2_linux-x64_bin.tar.gz
sudo tar xzvf apache-maven-3.8.4-bin.tar.gz
sudo cp -r jdk-17.0.2/ /opt/jdk17
sudo update-alternatives --install /usr/bin/java java /opt/jdk17/bin/java 100
sudo cp -r apache-maven-3.8.4/ /opt/apache-maven-3.8.4
sudo update-alternatives --install /usr/bin/mvn mvn /opt/apache-maven-3.8.4/bin/mvn 100
export JAVA_HOME=/opt/jdk17
export PATH=$PATH:$JAVA_HOME/bin
export PATH=/opt/apache-maven-3.8.4/bin:$PATH
git clone https://github.com/technionRamy/Nova.git
#taskset --cpu-list 24,25,26,27,28,29,30,31 