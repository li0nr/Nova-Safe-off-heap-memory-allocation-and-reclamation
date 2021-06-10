package com.yahoo.oak;

import java.util.concurrent.ConcurrentSkipListMap;

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
	    BST.containsKey(x,0);	    BST.Print();
	    Buff xy =new Buff();
	    Buff z= new Buff();
	    xy.set(110);
	    BST.put(xy,xy,0);
	    assert BST.containsKey(xy,0) == true && xy.get() == 110;
	    
	    BST.Print();
	    BST.containsKey(x,0);
	   
	    BST.remove(x, 0);
	    assert BST.containsKey(x,0) == false;
		    
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
	    
	    BST.containsKey(x,0);
	   
	    BST.remove(x, 0);
	    BST.containsKey(x,0);
	    BST.containsKey(z,0);
	    BST.put(z, x,0);
	    BST.containsKey(z,0);
	    x.compareTo(BST.get(z, 0));
		    
	  }
	
	
	@Test
	public void BST_GC_CoverTest() {

	    BST<Buff,Buff> BST = 
	    		new BST<Buff, Buff>();
	    	    
	    for(int i =0; i < 20; i++) {
		    Buff x =new Buff();
		    x.set(i);
		    BST.put(x, x);
		    assert BST.get(x).compareTo(x) == 0;
		    assert BST.containsKey(x) == true;
	    }
	    Buff x = new Buff();
	    for(int i =0; i < 20; i++) {
	    	x.set(i);
		    assert BST.containsKey(x) == true;
	    }
	    for(int i = 10; i < 20; i++) {
	    	x.set(i);
		    assert BST.remove(x) == true;
	    }
	    for(int i = 10; i < 20; i++) {
	    	x.set(i);
		    assert BST.containsKey(x) == false;
	    }
	    for(int i = 0; i < 10; i++) {
	    	x.set(i);
		    assert BST.containsKey(x) == true;
	    }


		    
	  }
	
	@Test
	public void BST_Fill_check() {
		
    
		final NativeMemoryAllocator allocator = new NativeMemoryAllocator(Integer.MAX_VALUE);
	    final NovaManager novaManager = new NovaManager(allocator);
	    

	    BST_Nova<Buff,Buff> BST = new BST_Nova<Buff,Buff>( Buff.DEFAULT_SERIALIZER, Buff.DEFAULT_SERIALIZER, 
	    												   Buff.DEFAULT_C, Buff.DEFAULT_C, novaManager);
	    
	    BST_HE<Buff,Buff> BST_HE = 
	    		new BST_HE<Buff, Buff>(Buff.DEFAULT_SERIALIZER, Buff.DEFAULT_SERIALIZER
	    							,Buff.DEFAULT_C, Buff.DEFAULT_C, allocator);
	    int i = 0;
	    Buff k = new Buff();
	    while(i < 100) {
	    	k.set(i);
	    	i++;
		    BST.put(k, k, 0);
		    BST_HE.put(k, k, 0);
	    }
	}
	
	@Test
	public void BST_Fill_delete() {
		final NativeMemoryAllocator allocator = new NativeMemoryAllocator(Integer.MAX_VALUE);
	    final NovaManager novaManager = new NovaManager(allocator);
	    

	    BST_Nova<Buff,Buff> BST = new BST_Nova<Buff,Buff>( Buff.DEFAULT_SERIALIZER, Buff.DEFAULT_SERIALIZER, 
	    												   Buff.DEFAULT_C, Buff.DEFAULT_C, novaManager);
	    
	    BST_HE<Buff,Buff> BST_HE = 
	    		new BST_HE<Buff, Buff>(Buff.DEFAULT_SERIALIZER, Buff.DEFAULT_SERIALIZER
	    							,Buff.DEFAULT_C, Buff.DEFAULT_C, allocator);
	    int i = 0;
	    Buff k = new Buff();
	    while(i < 200) {
	    	k.set(i);
	    	i++;
	    	BST.put(k, k, 0);
		    BST_HE.put(k, k, 0);
	    }
	    i = 0;
	    while(i < 200) {
	    	k.set(i);
	    	i++;
		    assert BST.remove( k, 0) == true;
		    assert BST_HE.remove( k, 0) == true;

	    }
	    BST_HE.HE.ForceCleanUp();
	    novaManager.ForceCleanUp();
	    assert allocator.allocated() == 0;
	    
	}
	
	
}