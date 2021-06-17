package com.yahoo.oak;

import java.util.ArrayList;
import java.util.Random;

import org.junit.Test;

import com.yahoo.oak.BST.BST;
import com.yahoo.oak.BST.BST_HE;
import com.yahoo.oak.BST.BST_Nova;
import com.yahoo.oak.Buff.Buff;

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
	    
	    BST.containsKey(x,0);
	   
	    BST.remove(x, 0);
	    assert BST.containsKey(x,0) == false;
		    
	  }
	
	
	@Test
	public void BST_GC_CoverTest() {

	    BST<Buff,Buff> BST = 
	    		new BST<Buff, Buff>(Buff.CC,Buff.CC);
	    	    
	    for(int i =0; i < 20; i++) {
		    Buff x =new Buff();
		    Buff v = new Buff();
		    v.set(i);
		    x.set(i);
		    BST.put(x, v);
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
	    k.set(0);
    	BST.put(k, k, 0);
    	BST.put(k, k, 0);
    	BST.remove( k, 0);
    	BST.remove( k, 0);

    	BST_HE.put(k, k, 0);
    	BST_HE.put(k, k, 0);
    	BST_HE.remove( k, 0);
    	BST_HE.remove( k, 0);


	    while(i < 1) {
	    	k.set(i);
	    	i++;
	    	assert BST.put(k, k, 0) == null;
		    assert BST_HE.put(k, k, 0) == null;
	    }
	    i = 0;
	    while(i < 1) {
	    	k.set(i);
	    	i++;
	    	assert BST.remove( k, 0) == true;
		    assert BST_HE.remove( k, 0) == true;

	    }
	    BST_HE.getHE().ForceCleanUp();
	    novaManager.ForceCleanUp();
	    assert allocator.allocated() == 0;
	    
	}
	
	@Test
	public void BST_Nova_stress() {
		final NativeMemoryAllocator allocator = new NativeMemoryAllocator(Integer.MAX_VALUE);
	    final NovaManager novaManager = new NovaManager(allocator);
	    

	    BST_Nova<Buff,Buff> BST = new BST_Nova<Buff,Buff>( Buff.DEFAULT_SERIALIZER, Buff.DEFAULT_SERIALIZER, 
	    												   Buff.DEFAULT_C, Buff.DEFAULT_C, novaManager);
	    	    
	    int i = 0;
	    ArrayList<Integer> list = new ArrayList<>();
	    Buff key = new Buff();
	    key.set(0);
    	BST.put(key,key,0);
    	BST.put(key,key,0);

	    Random x = new Random();
	    while (i < 50000) {
	    	int z  =x.nextInt(10000);
	    	list.add(z);
	    	key.set(z);
	    	BST.put(key,key,0);
	    	i++;
	    }
	    
	    while (i < 5000) {
	    	int z  =x.nextInt(20000);
	    	if(list.contains(z))
	    		list.remove((Integer)z);
	    	key.set(z);
	    	assert BST.remove(key ,0) == true;
	    	i++;
	    }
	    
	    for( Integer t : list) {
	    	assert BST.containsKey(key, 0);
	    }
		    
	  }
	
	
}
