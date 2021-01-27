#!/bin/bash
#java -XX:NativeMemoryTracking=summary
date >> $1
echo "**********" >> $1
echo "top:" >> $1
echo "	Virt" >> $1 | top -b -n1 | grep java | awk '{print $5}'>>$1 
echo "	Rss" >> $1 |top -b -n1 | grep java | awk '{print $6}'>>$1 
echo "**********" >> $1
PID=$(top -b -n1 | grep java | awk '{ print $1}')
echo "jcmd:" >> $1
jcmd $PID VM.native_memory summary | grep Total >> $1
echo "**********" >> $1
echo "pmap:" >> $1
pmap $PID | grep total >> $1
echo "**********" >> $1
