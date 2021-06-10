package com.yahoo.oak;


import com.yahoo.oak.HazardEras.HEslice;

import sun.misc.Unsafe;

public class Facade_HE <T,K> {

	static final int INVALID_BLOCKID=0;
	static final int INVALID_OFFSET=-1;
	static final int INVALID_VERSION=0;
	static final int INVALID_HEADER=0;
	static final int DELETED=1;
			
	
	static final Unsafe UNSAFE=UnsafeUtils.unsafe;
	
	static NovaManager novaManager;
	static HazardEras  _HazardEras;

	public Facade_HE(NativeMemoryAllocator allocator) {
		_HazardEras = new HazardEras(1, 32, allocator);
	}

		
	static public HEslice allocate(int size) {
		return _HazardEras.allocate(size);
		}
	
	static public void Delete (int idx, HEslice slice) {
		_HazardEras.retire(idx, slice);
	}
	
	
		
	
	static public <T> T Read(NovaSerializer<T> lambda, HEslice slice, int tidx) {
	
		if(slice.getdelEra() == -1)
			throw new IllegalArgumentException("slice deleted");
		
		HEslice toRead = _HazardEras.get_protected(slice, 1, tidx);
		if(toRead.getdelEra() == -1)
			throw new IllegalArgumentException("slice deleted");


		T obj = lambda.deserialize(toRead.address+toRead.offset);
		
		_HazardEras.clear(tidx);
		return obj;
	}

	
	static public <T> HEslice Write(NovaSerializer<T> lambda, T obj, HEslice slice, int tidx ) {
		if(slice.getdelEra() == -1)
			throw new IllegalArgumentException("slice deleted");
		
		HEslice toRead = _HazardEras.get_protected(slice, 1, tidx);
		if(toRead.getdelEra() == -1)
			throw new IllegalArgumentException("slice deleted");
		
		
		lambda.serialize(obj,slice.address+slice.offset);
		_HazardEras.clear(tidx);

		 return slice;
	}
	
	
	static public <T> HEslice WriteFast(NovaSerializer<T> lambda, T obj, HEslice slice, int tidx ) {
		if(slice.getdelEra() == -1)
			throw new IllegalArgumentException("slice deleted");
		
		lambda.serialize(obj,slice.address+slice.offset);

		 return slice;
	}
	
	
	 static public <T> int Compare(T obj, NovaC<T> srZ, HEslice slice, int tidx) {
			
			if(slice.getdelEra() == -1)
				throw new IllegalArgumentException("slice deleted");
			
			HEslice toRead = _HazardEras.get_protected(slice, 1, tidx);
			if(toRead.getdelEra() == -1)
				throw new IllegalArgumentException("slice deleted");


			int res = srZ.compareKeys(slice.address+slice.offset, obj);
			
			_HazardEras.clear(tidx);
			return res;	
		}
	 
	 
	 
	 static public <T> void Print(NovaC<T> srZ, HEslice slice) {
		 srZ.Print(slice.address+slice.offset);
		 }

}
