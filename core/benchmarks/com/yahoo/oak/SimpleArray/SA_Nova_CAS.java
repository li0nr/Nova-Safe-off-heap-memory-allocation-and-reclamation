package com.yahoo.oak.SimpleArray;


import java.util.Arrays;
import com.yahoo.oak.Facade_Slice;
import com.yahoo.oak.Facade_Slice.Facade_slice;
import com.yahoo.oak.NativeMemoryAllocator;
import com.yahoo.oak.NovaIllegalAccess;
import com.yahoo.oak.NovaManager;
import com.yahoo.oak.NovaS;

public class SA_Nova_CAS {
	

	private static final int DEFAULT_CAPACITY=10;
    final NativeMemoryAllocator allocator = new NativeMemoryAllocator(Integer.MAX_VALUE);
    final NovaManager mng = new NovaManager(allocator);
    
	

    private final NovaS srZ;
	private int size=0;
	private Facade_slice[] Slices;

	
	public SA_Nova_CAS(NovaS srZ){
		new Facade_Slice(mng);
		Slices = new Facade_slice[DEFAULT_CAPACITY];
		this.srZ = srZ;
	}
	
	public SA_Nova_CAS(int capacity,NovaS srZ ){
		new Facade_Slice(mng);
		Slices = new Facade_slice[capacity];
		this.srZ = srZ;

	}

	public <T> boolean fill(T e, int threadIDX) {
		if(size == Slices.length) {
			EnsureCap();//might be problematic 
		}
		Slices[size] = new Facade_slice();
		Facade_Slice.AllocateSlice(Slices[size],srZ.calculateSize(e), threadIDX);
		Facade_Slice.WriteFast(srZ,e,Slices[size],threadIDX);
		size++;
		return true;
	}
	
	public Facade_slice get(int index, int threadIDX) {
		return Slices[index];
	}
	

	public <T> boolean set(int index, T obj, int threadIDX)  {
		if(Slices[index].isDeleted())
				if(!Facade_Slice.AllocateSliceCAS(Slices[index],srZ.calculateSize(obj),threadIDX))
					return false;
		try{
			Facade_Slice.WriteFull(srZ, obj, Slices[index], threadIDX);
			return true;
		}catch(NovaIllegalAccess e) {
			return false;
		}
	}
	

	public boolean delete(int index, int threadIDX) {
		if(Slices[index].isDeleted())
			return false;
		if(Facade_Slice.DeleteCAS(threadIDX, Slices[index])){
			return true;
		}
		return false;
	}

	
	public int getSize(){
		return size;
	}

	
	private void EnsureCap() {
		int newSize = Slices.length *2;
		Slices = Arrays.copyOf(Slices, newSize);
	}
	
}