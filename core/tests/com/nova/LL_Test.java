package com.nova;

import org.junit.Test;

import com.nova.Buff.Buff;
import com.nova.LL.HarrisLinkedList;
import com.nova.LL.EBR.LL_EBR_CAS;
import com.nova.LL.EBR.LL_EBR_CAS_opt;
import com.nova.LL.EBR.LL_EBR_noCAS_opt;
import com.nova.LL.HE.HarrisLinkedListHE;
import com.nova.LL.HE.LL_HE_CAS_opt;
import com.nova.LL.HE.LL_HE_noCAS;
import com.nova.LL.Nova.LL_Nova_noCAS;
import com.nova.LL.Nova.LL_Nova_primitive_CAS;
import com.nova.LL.Nova.LL_Nova_primitive_noCAS_Magic;


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
	    LL_Nova_primitive_CAS<Buff,Buff> List = new LL_Nova_primitive_CAS<>(novaManager, Buff.DEFAULT_C, Buff.DEFAULT_SERIALIZER
	    		,Buff.DEFAULT_C, Buff.DEFAULT_SERIALIZER);
	    
	    Buff x =new Buff(4);
	    x.set(88);
		List.add(x,x,0);
		assert List.contains(x, 0) == true;
		x.set(120);
		List.add(x,x,0);
		assert List.contains(x, 0) == true;

	    Buff xy =new Buff(4);
	    Buff z= new Buff(128);
	    xy.set(110);
	    List.add(xy,xy,0);
	    List.contains(x,0);
		assert List.contains(x, 0) == true;
	    assert List.remove(x,0) == true;

	    assert List.contains(x,0) == false; 
	    assert List.contains(z,0) == false; 
	   // List.Print();
	}
	
	@Test
	public void LL_Nova_slice() {
	    final NativeMemoryAllocator allocator = new NativeMemoryAllocator(Integer.MAX_VALUE);
	    final NovaManager novaManager = new NovaManager(allocator);
	    LL_Nova_noCAS<Buff,Buff> List = new LL_Nova_noCAS<>(novaManager, Buff.DEFAULT_C, Buff.DEFAULT_SERIALIZER
	    		,Buff.DEFAULT_C, Buff.DEFAULT_SERIALIZER);
	    
	    Buff x =new Buff(4);
	    x.set(88);
		List.add(x,x,0);
		assert List.contains(x, 0) == true;
		x.set(120);
		List.add(x,x,0);
		assert List.contains(x, 0) == true;

	    Buff xy =new Buff(4);
	    Buff z= new Buff(128);
	    xy.set(110);
	    List.add(xy,xy,0);
	    List.contains(x,0);
		assert List.contains(x, 0) == true;
	    assert List.remove(x,0) == true;

	    assert List.contains(x,0) == false; 
	    assert List.contains(z,0) == false; 
	   // List.Print();
	}
	
	@Test
	public void LL_Nova_Magic() {
	    final NativeMemoryAllocator allocator = new NativeMemoryAllocator(Integer.MAX_VALUE);
	    final NovaManager novaManager = new NovaManager(allocator);
	    LL_Nova_primitive_noCAS_Magic<Buff,Buff> List = new LL_Nova_primitive_noCAS_Magic<>(novaManager, Buff.DEFAULT_C, Buff.DEFAULT_SERIALIZER
	    		,Buff.DEFAULT_C, Buff.DEFAULT_SERIALIZER);
	    
	    Buff x =new Buff(4);
	    x.set(88);
		List.add(x,x,0);
		assert List.contains(x, 0) == true;
		x.set(120);
		List.add(x,x,0);
		assert List.contains(x, 0) == true;

	    Buff xy =new Buff(4);
	    Buff z= new Buff(128);
	    xy.set(110);
	    List.add(xy,xy,0);
	    List.contains(x,0);
		assert List.contains(x, 0) == true;
	    assert List.remove(x,0) == true;

	    assert List.contains(x,0) == false; 
	    assert List.contains(z,0) == false; 
	  //  List.Print();
	}
	
	@Test
	public void LL_GC() {
	    HarrisLinkedList<Buff,Buff> List = new HarrisLinkedList<>(Buff.CC,Buff.CC);
	    Buff x =new Buff(4);
	    x.set(88);
		List.add(x,x,0);
		assert List.contains(x, 0) == true;
		x.set(120);
		List.add(x,x,0);
		assert List.contains(x, 0) == true;

	    Buff xy =new Buff(4);
	    Buff z= new Buff(128);
	    xy.set(110);
	    List.add(xy,xy,0);
	    List.contains(x,0);
	    assert (int)List.get(x, Buff.GCR, 0) == 120;

		assert List.contains(x, 0) == true;
	    assert List.remove(x,0) == true;

	    assert List.contains(x,0) == false; 
	    assert List.contains(z,0) == false;
	    

	}
	
	
	@Test
	public void LL_EBR(){
	    final NativeMemoryAllocator allocator = new NativeMemoryAllocator(Integer.MAX_VALUE);
	    final NovaManager novaManager = new NovaManager(allocator);
	    LL_EBR_CAS_opt<Buff,Buff> List = new LL_EBR_CAS_opt<>(allocator, Buff.DEFAULT_C, Buff.DEFAULT_SERIALIZER, Buff.DEFAULT_C, Buff.DEFAULT_SERIALIZER);

	    Buff x =new Buff(4);
	    x.set(88);
		List.add(x,x,0);
		assert List.contains(x, 0) == true;
		x.set(120);
		List.add(x,x,0);
		assert List.contains(x, 0) == true;

	    Buff xy =new Buff(4);
	    Buff z= new Buff(128);
	    xy.set(110);
	    List.add(xy,xy,0);
	    List.contains(x,0);
		assert List.contains(x, 0) == true;
	    assert List.remove(x,0) == true;

	    assert List.contains(x,0) == false; 
	    assert List.contains(z,0) == false; 
	}
	
	
	@Test
	public void BST_Fill_delete() {
	    final NativeMemoryAllocator allocator = new NativeMemoryAllocator(Integer.MAX_VALUE);
	    final NovaManager novaManager = new NovaManager(allocator);
	    LL_Nova_noCAS<Buff,Buff> List = new LL_Nova_noCAS<>(novaManager, Buff.DEFAULT_C, Buff.DEFAULT_SERIALIZER
	    		,Buff.DEFAULT_C, Buff.DEFAULT_SERIALIZER);
	    HarrisLinkedListHE<Buff>List_HE = new HarrisLinkedListHE<>(allocator, Buff.DEFAULT_C, Buff.DEFAULT_SERIALIZER);
	    LL_EBR_CAS_opt<Buff,Buff> EBR_CAS = new LL_EBR_CAS_opt<>(allocator, Buff.DEFAULT_C, Buff.DEFAULT_SERIALIZER
	    		,Buff.DEFAULT_C, Buff.DEFAULT_SERIALIZER);
	    LL_HE_CAS_opt<Buff,Buff> HE_CAS= new LL_HE_CAS_opt<>(allocator, Buff.DEFAULT_C, Buff.DEFAULT_SERIALIZER
	    		,Buff.DEFAULT_C, Buff.DEFAULT_SERIALIZER);
	    int i = 0;
	    Buff k = new Buff();
	    while(i < 200) {
	    	k.set(i);
	    	i++;
	    	List.add(k,k, 0);
	    	EBR_CAS.add(k, k, 0);
	    	List_HE.add(k, 0);
	    }
	    i = 0;
	    while(i < 200) {
	    	k.set(i);
	    	i++;
		    assert List.remove( k, 0) == true;
		    assert List_HE.remove( k, 0) == true;
		    assert EBR_CAS.remove( k, 0) == true;


	    }
	    List_HE.getHE().ForceCleanUp();
	    EBR_CAS.ForceCleanUp();
	    novaManager.ForceCleanUp();
	    assert allocator.allocated() == 0;
	}
	
	
	@Test
	public void LL_Fill_delete_EBR() {
	    final NativeMemoryAllocator allocator = new NativeMemoryAllocator(Integer.MAX_VALUE);
	    LL_EBR_CAS_opt<Buff,Buff> Listopt = new LL_EBR_CAS_opt<>(allocator, Buff.DEFAULT_C, Buff.DEFAULT_SERIALIZER
	    		,Buff.DEFAULT_C, Buff.DEFAULT_SERIALIZER);
	    LL_EBR_CAS<Buff,Buff> list= new LL_EBR_CAS<>(allocator, Buff.DEFAULT_C, Buff.DEFAULT_SERIALIZER
	    		,Buff.DEFAULT_C, Buff.DEFAULT_SERIALIZER);
	    int i = 0;
	    Buff k = new Buff();
	    while(i < 200) {
	    	k.set(i);
	    	i++;
	    	Listopt.add(k,k, 0);
	    	list.add(k,k, 0);

	    }
	    i = 0;
	    while(i < 200) {
	    	k.set(i);
	    	i++;
		    assert Listopt.remove( k, 0) == true;
		    assert list.remove( k, 0) == true;

	    }
	    Listopt.ForceCleanUp();
	    list.ForceCleanUp();
	    assert allocator.allocated() == 0;
	}
	
	
	@Test
	public void LL_HE_CAS_opt(){
	    final NativeMemoryAllocator allocator = new NativeMemoryAllocator(Integer.MAX_VALUE);
	    LL_HE_noCAS<Buff,Buff> List = new LL_HE_noCAS<>(allocator, Buff.DEFAULT_C, Buff.DEFAULT_SERIALIZER, Buff.DEFAULT_C, Buff.DEFAULT_SERIALIZER);

	    Buff x =new Buff(4);
	    x.set(88);
		List.add(x,x,0);
		assert List.contains(x, 0) == true;
		x.set(120);
		List.add(x,x,0);
		assert List.contains(x, 0) == true;

	    Buff xy =new Buff(4);
	    Buff z= new Buff(128);
	    xy.set(110);
	    List.add(xy,xy,0);
	    List.contains(x,0);
		assert List.contains(x, 0) == true;
	    assert List.remove(x,0) == true;

	    assert List.contains(x,0) == false; 
	    assert List.contains(z,0) == false; 
	}
	
	@Test
	public void LL_EBR_noCAS_opt(){
	    final NativeMemoryAllocator allocator = new NativeMemoryAllocator(Integer.MAX_VALUE);
	    LL_EBR_noCAS_opt<Buff,Buff> List = new LL_EBR_noCAS_opt<>(allocator, Buff.DEFAULT_C, Buff.DEFAULT_SERIALIZER, Buff.DEFAULT_C, Buff.DEFAULT_SERIALIZER);

	    Buff x =new Buff(4);
	    x.set(88);
		List.add(x,x,0);
		assert List.contains(x, 0) == true;
		x.set(120);
		List.add(x,x,0);
		assert List.contains(x, 0) == true;

	    Buff xy =new Buff(4);
	    Buff z= new Buff(128);
	    xy.set(110);
	    List.add(xy,xy,0);
	    List.contains(x,0);
		assert List.contains(x, 0) == true;
	    assert List.remove(x,0) == true;

	    assert List.contains(x,0) == false; 
	    assert List.contains(z,0) == false; 
	}
}
