package com.yahoo.oak;

import java.util.concurrent.atomic.AtomicLong; 

import  sun.misc.Unsafe;

public class SimplePair{
	int first, second;

	SimplePair(){
		first=0;
		second=-1;
	}
	
	SimplePair(AtomicLong f){
		long n=f.get();
		SimplePair a=extractversion(n);
		first=a.getfirst();
		second=a.getsecond();
	}
	
	SimplePair(long f){
		SimplePair a=extractversion(f);
		first=a.getfirst();
		second=a.getsecond();
	}
	
	SimplePair(int f, int s){
		first=f;
		second=s;
	}
	SimplePair(int f){
		first=f;
		second=1;
	}
	public int getfirst() {
		return first;
	}
	public int getsecond() {
		return second;
	}
	public boolean equals(SimplePair a) {
		if(first == a.getfirst() && second == a.getsecond()) return true;
		else return false;
	}
	
	
	
	private SimplePair extractversion(long toExtract) {
		int version=(int) (toExtract >>1 )&0x7FFFF;
		int del= (int) (toExtract)& 0x1;
		return new SimplePair(version,del);
	}
}