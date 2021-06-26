package com.yahoo.oak;


import com.yahoo.oak.HazardEras.HEslice;

import sun.misc.Unsafe;

public class Facade_HE <T,K> {

	static final Unsafe UNSAFE=UnsafeUtils.unsafe;
	static HazardEras  _HazardEras;

	public Facade_HE(NativeMemoryAllocator allocator) {
		_HazardEras = new HazardEras(32, allocator);
	}

		
	static public HEslice allocate(int size) {
		return _HazardEras.allocate(size);
		}
	
	static public HEslice allocateCAS(int size) {
		return _HazardEras.allocateCAS(size);
		}
	
	static public void Delete (int idx, HEslice slice) {
		_HazardEras.retire(idx, slice);
		}
	
	static public void DeleteCAS (int idx, HEslice slice) {
		_HazardEras.retireCAS(idx, slice);
		}
	
		
	
	static public <T> T Read(NovaS<T> lambda, HEslice slice, int tidx) {
	
		if(slice.getdelEra() != -1)
			throw new NovaIllegalAccess();
		
		HEslice toRead = _HazardEras.get_protected(slice, 1, tidx);
		if(slice.getdelEra() != -1)
			throw new NovaIllegalAccess();


		T obj = lambda.deserialize(toRead.address+toRead.offset);
		
		_HazardEras.clear(tidx);
		return obj;
	}

	
	static public <T> HEslice Write(NovaS<T> lambda, T obj, HEslice slice, int tidx ) {
		if(slice.getdelEra() != -1)
			throw new NovaIllegalAccess();
		
		HEslice toRead = _HazardEras.get_protected(slice, 1, tidx);
		if(toRead.getdelEra() != -1)
			throw new NovaIllegalAccess();
		
		
		lambda.serialize(obj,slice.address+slice.offset);
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
			
			if(slice.getdelEra() != -1)
				throw new NovaIllegalAccess();
			
			HEslice toRead = _HazardEras.get_protected(slice, 1, tidx);
			if(toRead.getdelEra() != -1)
				throw new NovaIllegalAccess();


			int res = srZ.compareKeys(slice.address+slice.offset, obj);
			
			_HazardEras.clear(tidx);
			return res;	
		}
	 
	 static public void fastFree(NovaSlice S) {
		 _HazardEras.fastFree(S);
	 }
	 
	 
	 static public <T> void Print(NovaC<T> srZ, HEslice slice) {
		 srZ.Print(slice.address+slice.offset);
		 }

}
