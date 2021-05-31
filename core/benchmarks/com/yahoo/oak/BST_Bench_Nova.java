package com.yahoo.oak;

public class BST_Bench_Nova {
	
	
	
	public static void main(String[] args) {
		int BST_SIZE= 10000;

	    final NativeMemoryAllocator allocator = new NativeMemoryAllocator(Integer.MAX_VALUE);
	    final NovaManager mng = new NovaManager(allocator);
	    
	    BST_Nova<Buff, Buff>BST = new BST_Nova<Buff, Buff>(Buff.DEFAULT_SERIALIZER, Buff.DEFAULT_SERIALIZER
									,Buff.DEFAULT_C, Buff.DEFAULT_C, mng);
	    
	    
		for (int i=0; i <BST_SIZE ; i++) {
			Buff k = new Buff();
			Buff v = new Buff();
			k.set(i);
			v.set(BST_SIZE);
			BST.put(k,v, 0);
		}
		Buff k = new Buff();
		k.set(0);
		System.out.print("warmup \n");
		int i = 0;
		while(i <1000) {
			BST.get(k, 0);
			i++;
		}
		System.out.print("real \n");
		long start = System.nanoTime();
		BST.get(k, 0);
		long End = System.nanoTime();
		long x = End - start;
		System.out.print("curr time is" + x);
		
		
	}

}
