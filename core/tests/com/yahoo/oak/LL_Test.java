package com.yahoo.oak;

import org.junit.Test;

public class LL_Test {
	
	@Test
	public void LL_HE(){
	    final NativeMemoryAllocator allocator = new NativeMemoryAllocator(Integer.MAX_VALUE);
	    final NovaManager novaManager = new NovaManager(allocator);
	    HarrisLinkedListHE<Buff> List = new HarrisLinkedListHE<>(allocator, Buff.DEFAULT_C, Buff.DEFAULT_SERIALIZER);

	    Buff x =new Buff(4);
	    x.set(88);
		List.add(x,0);
		assert List.contains(x, 0) == true;
		x.set(120);
		List.add(x,0);
		assert List.contains(x, 0) == true;

	    Buff xy =new Buff(4);
	    Buff z= new Buff(128);
	    xy.set(110);
	    List.add(xy,0);
	    List.contains(x,0);
		assert List.contains(x, 0) == true;
	    assert List.remove(x,0) == true;

	    assert List.contains(x,0) == false; 
	    assert List.contains(z,0) == false; 
	}
	
	@Test
	public void LL_Nova() {
	    final NativeMemoryAllocator allocator = new NativeMemoryAllocator(Integer.MAX_VALUE);
	    final NovaManager novaManager = new NovaManager(allocator);
	    HarrisLinkedListNova<Buff> List = new HarrisLinkedListNova<>(novaManager, Buff.DEFAULT_C, Buff.DEFAULT_SERIALIZER);
	    
	    Buff x =new Buff(4);
	    x.set(88);
		List.add(x,0);
		assert List.contains(x, 0) == true;
		x.set(120);
		List.add(x,0);
		assert List.contains(x, 0) == true;

	    Buff xy =new Buff(4);
	    Buff z= new Buff(128);
	    xy.set(110);
	    List.add(xy,0);
	    List.contains(x,0);
		assert List.contains(x, 0) == true;
	    assert List.remove(x,0) == true;

	    assert List.contains(x,0) == false; 
	    assert List.contains(z,0) == false; 
	}
}
