package com.nova;

import java.util.function.Function;

import com.nova.EBR.EBRslice;
import sun.misc.Unsafe;

public class Facade_EBR {

	static final Unsafe UNSAFE=UnsafeUtils.unsafe;
	static EBR  _EBR;

	public Facade_EBR(NativeMemoryAllocator allocator) {
		_EBR = new EBR(allocator);
	}

		
	static public EBRslice allocate(int size) {
		return _EBR.allocate(size);
		}
	
	static public EBRslice allocateCAS(int size) {
		return _EBR.allocateCAS(size);
		}
	
	static public void Delete (int idx, EBRslice slice) {
		_EBR.retire(slice, idx);
		}
	
	static public <T> T Read(NovaR<T> Reader, EBRslice slice, int tidx) {
		_EBR.start_op(tidx);
		
		if(slice.getEpoch() != -1)
			 throw new NovaIllegalAccess();

		T obj = Reader.apply(slice.address+slice.offset);
		
		_EBR.clear(tidx);
		return obj;
	}

	
	static public <T> EBRslice Write(NovaS<T> lambda, T obj, EBRslice slice, int tidx ) {
		
		_EBR.start_op(tidx);
		
		if(slice.getEpoch() != -1)
			 throw new NovaIllegalAccess();
		
		lambda.serialize(obj,slice.address+slice.offset);
		_EBR.clear(tidx);

		 return slice;
	}
	
	static public <T> EBRslice OverWrite(Function< Long, Long> lambda, EBRslice slice, int tidx ) {
		
		_EBR.start_op(tidx);
		
		if(slice.getEpoch() != -1)
			 throw new NovaIllegalAccess();
		
		lambda.apply(slice.address+slice.offset);
		_EBR.clear(tidx);

		 return slice;
	}
	
	
	static public <T> EBRslice WriteFast(NovaS<T> lambda, T obj, EBRslice slice, int tidx ) {
		if(slice.getEpoch() != -1)
			 throw new NovaIllegalAccess();
		
		lambda.serialize(obj,slice.address+slice.offset);
		return slice;
	}
	
	
	 static public <T> int Compare(T obj, NovaC<T> cmP, EBRslice slice, int tidx) {
			
		 _EBR.start_op(tidx);
			
		 if(slice.getEpoch() != -1)
			 throw new NovaIllegalAccess();
		 
		 int res = cmP.compareKeys(slice.address+slice.offset, obj);
		 _EBR.clear(tidx);
		 return res;	
		 
	 }
	 
     static public void fastFree(NovaSlice s) {
   	  _EBR.fastFree(s);
     }
	 
     static public void ForceCleanUp() {
    	 _EBR.ForceCleanUp();
     }
	 
	 
	 static public <T> void Print(NovaC<T> srZ, EBRslice slice) {
		 srZ.Print(slice.address+slice.offset);
		 }

}
