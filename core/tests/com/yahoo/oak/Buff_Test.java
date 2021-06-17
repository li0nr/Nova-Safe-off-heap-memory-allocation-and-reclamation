package com.yahoo.oak;

import java.util.Random;

import org.junit.Test;

import com.yahoo.oak.Buff.Buff;

public class Buff_Test {
	
	@Test
	public void testDefaultBuff() {
		Buff x = new Buff();
		Random rand = new Random();
		int z = rand.nextInt();
		x.set(z);
		assert x.buffer.getInt(0) == z;
		
	}
	
	@Test
	public void testBuff() {
		
		Random rand = new Random();
		int size = rand.nextInt(20);
		Buff x = new Buff(Integer.BYTES * size);

		int z = rand.nextInt(10000);
		x.set(z);
		int i =0;
		while(i < size) {
			assert x.buffer.getInt(i*Integer.BYTES) == z+i;
			i++;
		}
	}

}
