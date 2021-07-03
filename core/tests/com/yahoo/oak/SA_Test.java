package com.yahoo.oak;

import org.junit.Test;

import com.yahoo.oak.Buff.Buff;
import com.yahoo.oak.SimpleArray.SA_EBR_CAS_opt;
import com.yahoo.oak.SimpleArray.SA_HE_CAS_opt;
import com.yahoo.oak.SimpleArray.SA_Nova_CAS;
import com.yahoo.oak.SimpleArray.SA_Nova_Primitive_CAS;


public class SA_Test {
	
	@Test
	public void SA_HE(){
		SA_HE_CAS_opt mySA = new SA_HE_CAS_opt(Buff.DEFAULT_SERIALIZER); 
	    Buff x =new Buff(4);

		
		for(int i =0; i < 1024; i++) {
			x.set(i);
			mySA.fill(x, 0);
		}
		
		for(int i =0; i < 1024; i++) {
			x.set(i);
		}
		
		for(int i =0; i < 1024; i+=2) {
			x.set(i);
			assert mySA.delete(i, 0) == true;
		}
		for(int i =1; i < 1024; i+=2) {
			x.set(i+4);
			assert mySA.set(i, x, 0) == true;
		}
		for(int i =1; i < 1024; i+=2) {
			x.set(i+4);
		}
		
	}
	
	@Test
	public void SA_EBR(){
		SA_EBR_CAS_opt mySA = new SA_EBR_CAS_opt(Buff.DEFAULT_SERIALIZER); 
	    Buff x =new Buff(4);

		
		for(int i =0; i < 1024; i++) {
			x.set(i);
			mySA.fill(x, 0);
		}
		
		for(int i =0; i < 1024; i++) {
			x.set(i);
		}
		
		for(int i =0; i < 1024; i+=2) {
			x.set(i);
			assert mySA.delete(i, 0) == true;
		}
		for(int i =1; i < 1024; i+=2) {
			x.set(i+4);
			assert mySA.set(i, x, 0) == true;
		}
		for(int i =1; i < 1024; i+=2) {
			x.set(i+4);
		}
		
	}
	
	@Test
	public void SA_Nova(){
		SA_Nova_CAS mySA = new SA_Nova_CAS(Buff.DEFAULT_SERIALIZER); 
	    Buff x =new Buff(100);

		
		for(int i =0; i < 1024; i++) {
			x.set(i);
			mySA.fill(x, 0);
		}
		
		for(int i =0; i < 1024; i++) {
			x.set(i);
			mySA.get(4, Buff.DEFAULT_R, 0);
		}
		
		for(int i =0; i < 1024; i+=2) {
			x.set(i);
			assert mySA.delete(i, 0) == true;
		}
		for(int i =1; i < 1024; i+=2) {
			x.set(i+4);
			assert mySA.set(i, x, 0) == true;
		}
		for(int i =1; i < 1024; i+=2) {
			x.set(i+4);
		}
		
	}
	
	
	@Test
	public void SA_Nova_primitive(){
		SA_Nova_Primitive_CAS mySA = new SA_Nova_Primitive_CAS(Buff.DEFAULT_SERIALIZER); 
	    Buff x =new Buff(12);

		
		for(int i =0; i < 1024; i++) {
			x.set(i);
			mySA.fill(x, 0);
		}
		
		for(int i =0; i < 1024; i++) {
			x.set(i);
			mySA.get(Buff.DEFAULT_R,i, 0);
		}
		
		for(int i =0; i < 1024; i+=2) {
			x.set(i);
			assert mySA.delete(i, 0) == true;
		}
		for(int i =1; i < 1024; i+=2) {
			x.set(i+4);
			assert mySA.set(i, x, 0) == true;
		}
		for(int i =1; i < 1024; i+=2) {
			x.set(i+4);
		}
		
	}
	
}