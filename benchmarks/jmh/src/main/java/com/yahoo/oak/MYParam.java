package com.yahoo.oak;

public class MYParam {
	static final int warmups=5;
	static final int iterations=5;
	
	static final int thread1=1;
	static final int thread2=2;
	static final int thread4=4;
	static final int thread8=8;
	static final int thread12=12;
	static final int thread16=16;
	static final int thread20=20;
	static final int thread24=24;
	static final int thread32=32;
 
	static final int G_LIST_SIZE = 1000000;
	static final int Limit = G_LIST_SIZE/4;
	static final int range = 2500;
	static final int forks = 0;
}
//java -jar -Xmx8g -XX:MaxDirectMemorySize=8g ./benchmarks/target/benchmarks.jar put -p numRows=500000 -prof stack