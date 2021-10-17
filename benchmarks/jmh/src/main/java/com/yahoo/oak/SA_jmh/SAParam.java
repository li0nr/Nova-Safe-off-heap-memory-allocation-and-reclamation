package com.yahoo.oak.SA_jmh;

public class SAParam {
	public static final int warmups=5;
	public static final int iterations=5;
	
	static final int thread1=1;
	static final int thread2=2;
	static final int thread4=4;
	static final int thread8=8;
	static final int thread12=12;
	static final int thread16=16;
	static final int thread20=20;
	static final int thread24=24;
	static final int thread32=32;
 	
	public static final int LL_Size = 5_000_000;

	public static final int forks = 0;
}
//java -jar -Xmx8g -XX:MaxDirectMemorySize=8g ./benchmarks/target/benchmarks.jar put -p numRows=500000 -prof stack