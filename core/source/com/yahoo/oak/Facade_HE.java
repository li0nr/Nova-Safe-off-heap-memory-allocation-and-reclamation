package com.yahoo.oak;


import java.util.function.Function;

import com.yahoo.oak.HazardEras.HEslice;

import sun.misc.Unsafe;

public class Facade_HE {

	static final Unsafe UNSAFE=UnsafeUtils.unsafe;
	static HazardEras  _HazardEras;

	public Facade_HE(NativeMemoryAllocator allocator) {
		_HazardEras = new HazardEras(allocator);
		}

	static public HEslice allocate(int size) {
		return _HazardEras.allocate(size);
		}
	
	static public void Delete (int idx, HEslice slice) {
		_HazardEras.retire(idx, slice);
		}
	

		
	static public <T> T Read(NovaR Reader, HEslice slice, int tidx) {
			
		HEslice toRead = _HazardEras.get_protected(slice, tidx);
		if(toRead.getdelEra() != -1)
			return null;

		T obj = (T)Reader.apply(toRead.address+toRead.offset);
		
		_HazardEras.clear(tidx);
		return obj;
	}

	
	static public <T> HEslice Write(NovaS<T> lambda, T obj, HEslice slice, int tidx ) {
		
		HEslice toRead = _HazardEras.get_protected(slice, tidx);
		if(toRead.getdelEra() != -1)
			return null;		
		
		lambda.serialize(obj,slice.address+slice.offset);
		_HazardEras.clear(tidx);

		 return slice;
	}
	
	static public <T> HEslice OverWrite(Function<Long, Long> lambda, HEslice slice, int tidx ) {
		
		HEslice toRead = _HazardEras.get_protected(slice, tidx);
		if(toRead.getdelEra() != -1)
			return null;		
		
		lambda.apply(slice.address+slice.offset);
		_HazardEras.clear(tidx);

		 return slice;
	}
	
	static public <T> HEslice WriteFast(NovaS<T> lambda, T obj, HEslice slice, int tidx ) {
		if(slice.getdelEra() != -1)
			throw new NovaIllegalAccess();
		lambda.serialize(obj,slice.address+slice.offset);
		 return slice;
	}
	
	
	 static public <T> int Compare(T obj, NovaC<T> srZ, HEslice slice, int tidx) {
						
			HEslice toRead = _HazardEras.get_protected(slice, tidx);
			if(toRead == null ||toRead.getdelEra() != -1 )
				throw new NovaIllegalAccess();

			int res = srZ.compareKeys(toRead.address+toRead.offset, obj);
			
			_HazardEras.clear(tidx);
			return res;	
		}
	 
	 static public void fastFree(NovaSlice S) {
		 _HazardEras.fastFree(S);
	 }
	 
	 
	 static public <T> void Print(NovaC<T> srZ, HEslice slice) {
		 srZ.Print(slice.address+slice.offset);
		 }
	 
	 
	 
	static public HEslice allocateCAS(int size) {
		return _HazardEras.allocateCAS(size);
		}
	static public void DeleteCAS (int idx, HEslice slice) {
		_HazardEras.retireCAS(idx, slice);
		}
		
}
