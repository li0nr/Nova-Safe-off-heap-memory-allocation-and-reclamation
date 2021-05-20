package com.yahoo.oak;

import org.junit.Test;

public class BST_Test {

	
	@Test
	public void BST_Nova_CoverTest() {
		final NativeMemoryAllocator allocator = new NativeMemoryAllocator(Integer.MAX_VALUE);
	    final NovaManager novaManager = new NovaManager(allocator);
	    

	    BST_Nova<Buff,Buff> BST = new BST_Nova<Buff,Buff>( Buff.DEFAULT_SERIALIZER, Buff.DEFAULT_SERIALIZER, 
	    												   Buff.DEFAULT_C, Buff.DEFAULT_C, novaManager);
	    	    
	    Buff x =new Buff();
	    x.set(88);
	    BST.put(x,x,0);
	    assert BST.containsKey(x,0) == true && x.get() == 88;

		x.set(120);
		BST.put(x,x,0);
	    BST.containsKey(x,0);
	    Buff xy =new Buff();
	    Buff z= new Buff();
	    xy.set(110);
	    BST.put(xy,xy,0);
	    assert BST.containsKey(xy,0) == true && xy.get() == 110;
	    BST.putIfAbsent(x, z,0);
	    
	    BST.containsKey(x,0);
	   
	    BST.remove(x, 0);
	    BST.containsKey(x,0);
	    BST.putIfAbsent(z, x,0);
	    assert BST.containsKey(z,0) == true && z.get() == 0;
	    assert x.compare(BST.get(z, 0)) == 0;
		    
	  }
	
	
	@Test
	public void BST_HE_CoverTest() {
		final NativeMemoryAllocator allocator = new NativeMemoryAllocator(Integer.MAX_VALUE);
		
	    BST_HE<Buff,Buff> BST = 
	    		new BST_HE<Buff, Buff>(Buff.DEFAULT_SERIALIZER, Buff.DEFAULT_SERIALIZER
	    							,Buff.DEFAULT_C, Buff.DEFAULT_C, allocator);
	    	    
	    Buff x =new Buff();
	    Buff z= new Buff();


	    x.set(88);
	    BST.put(x,x,0);
	    BST.containsKey(x,0);

		x.set(120);
		BST.put(x,x,0);
	    BST.containsKey(x,0);
	    BST.containsKey(z,0);

	    BST.get(x, 0);
	    Buff xy =new Buff();
	    xy.set(110);

	    BST.put(xy,xy,0);
	    BST.containsKey(z,0);

	    BST.containsKey(xy,0);
	    BST.putIfAbsent(x, z,0);
	    
	    BST.containsKey(x,0);
	   
	    BST.remove(x, 0);
	    BST.containsKey(x,0);
	    BST.containsKey(z,0);
	    BST.put(z, x,0);
	    BST.containsKey(z,0);
	    x.compare(BST.get(z, 0));
		    
	  }
}
