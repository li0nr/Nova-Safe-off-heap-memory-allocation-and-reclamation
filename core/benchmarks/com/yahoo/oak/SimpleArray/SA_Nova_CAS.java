package com.yahoo.oak.SimpleArray;


import java.util.Arrays;
import com.yahoo.oak.Facade_Slice;
import com.yahoo.oak.Facade_Slice.Facade_slice;
import com.yahoo.oak.NativeMemoryAllocator;
import com.yahoo.oak.NovaManager;
import com.yahoo.oak.NovaR;
import com.yahoo.oak.NovaS;
import com.yahoo.oak._Global_Defs;

public class SA_Nova_CAS {
	

	private static final int DEFAULT_CAPACITY=10;
    final NativeMemoryAllocator allocator = new NativeMemoryAllocator(_Global_Defs.MAXSIZE);
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
	
	public <R> R get(int index, NovaR Reader, int threadIDX) {
		return (R)Facade_Slice.Read(Reader,Slices[index]);
		}
	
	public <T> boolean set(int index, T obj, int threadIDX)  {
		Facade_slice toSet = Slices[index];
		if(toSet.isDeleted())
				if(!Facade_Slice.AllocateSliceCAS(toSet,srZ.calculateSize(obj),threadIDX))
					return false;
		return Facade_Slice.WriteFull(srZ, obj, toSet, threadIDX) != null ? true : false;
		}
	
	public boolean delete(int index, int threadIDX) {
		return Facade_Slice.DeleteCAS(threadIDX, Slices[index]);
		}

	public int getSize(){
		return size;
	}
	
	private void EnsureCap() {
		int newSize = Slices.length *2;
		Slices = Arrays.copyOf(Slices, newSize);
	}
	public NativeMemoryAllocator getAlloc() {
		return allocator;
	}
	
}