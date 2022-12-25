package com.nova;

import static org.junit.Assert.assertTrue;

import java.util.Random;

import org.junit.Test;

import com.nova.Buff.Buff;

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
	
	@Test
	public void testGCBuff() {
		Buff gc = new Buff(2024);
		long address = UnsafeUtils.unsafe.allocateMemory(2028);
		
		gc.set(4);
		Buff mygc = Buff.CC.Copy(gc);
		assertTrue(gc.equals(mygc));
		
		Buff.DEFAULT_SERIALIZER.serialize(gc, address);
		Integer mygcINT = (Integer)Buff.GCR.apply(mygc);
		
		Integer nativeINT = (Integer)Buff.DEFAULT_R.apply(address);
		assertTrue(mygcINT.equals(nativeINT));
		
		Buff off = Buff.DEFAULT_SERIALIZER.deserialize(address);
		assertTrue(gc.equals(off));

		
	}

}
