package com.yahoo.oak;

import java.util.function.Function;

import com.yahoo.oak.Facade_Slice.Facade_slice;

import sun.misc.Unsafe;

public class Facade_Slice_MagicNumber {

	static final int INVALID_BLOCKID=0;
	static final int INVALID_OFFSET=-1;
	static final int INVALID_VERSION=0;
	static final int INVALID_HEADER=0;
	static final int DELETED=1;
			
	static NovaManager novaManager;	
	
	static final Unsafe UNSAFE=UnsafeUtils.unsafe;

	

	
	public Facade_Slice_MagicNumber(NovaManager mng) {
		novaManager = mng;
	}


	
	static public <K> boolean AllocateSlice(Facade_slice S, int size, int idx) {
		int CurrVer = S.version;
		if(CurrVer%2 != DELETED)
			return false;
	
		NovaSlice 	newslice = novaManager.getSlice_Magic(size,idx);
		S.offset  =	newslice.offset;
		S.blockID =	newslice.blockID;
		S.address = newslice.address;
		S.length  = newslice.length;
		S.version = (int) newslice.getVersionMagic() &0xFFFFFF;
		assert S.blockID != 0;
		return true;
	}
	
	
	
	static public boolean Delete(int idx, Facade_slice S) {
		
		if(S.version%2 == DELETED)
			return false;		
		
		int localBlock = S.blockID;
		int localOffset = S.offset;
		int localLenght = S.length;
		long localAddress = S.address;
		
		long SliceHeaderAddress= localAddress+localOffset+NovaManager.MAGIC_SIZE;;

		long OffHeapMetaData= UNSAFE.getLong(SliceHeaderAddress);//reads off heap meta		
		
		if(NovaManager.MAGIC_NUM !=  UNSAFE.getLong(localAddress + localOffset) )
			return false;

		if(!UNSAFE.compareAndSwapLong(null, SliceHeaderAddress, OffHeapMetaData,
				OffHeapMetaData|1)) //swap with CAS
			 return false;
		S.version = S.version |1;
		novaManager.release(localBlock, localOffset, localLenght,idx); 
		return true; 
	}


	//assumes that the slice can be access solely through one thread which called this function
	static public <K> boolean DeletePrivate(int idx, Facade_slice S) {
		if(S.version%2 == DELETED)  //we can also not use this but its here for sanity!
			return false;		 
		novaManager.free(S);
		return true; 
	}
	
	
	static public <T> Facade_slice OverWrite (Function<Long,Long> lambda, Facade_slice S ,int idx) {//for now write doesnt take lambda for writing 

		if(S.version%2 == DELETED)
			return null;
		
		long facadeRef	= _Global_Defs.buildRef		(S.blockID,S.offset);
		
		if(bench_Flags.TAP) {
			novaManager.setTap(facadeRef,idx);	
			if(bench_Flags.Fences)UNSAFE.fullFence();
			}
		if(NovaManager.MAGIC_NUM !=  UNSAFE.getLong(S.address + S.offset) )
			return null;
		
		if(! (S.version == (int)(UNSAFE.getLong(S.address+S.offset+NovaManager.MAGIC_SIZE)&0xFFFFFF))) {
			novaManager.UnsetTap(idx);
			return null;
			}
		 lambda.apply(S.address + S.offset + NovaManager.HEADER_SIZE +NovaManager.MAGIC_SIZE);
		 if(bench_Flags.TAP) {
             if(bench_Flags.Fences)UNSAFE.storeFence();
            novaManager.UnsetTap(idx);
            }
		 return S;
	}
	
	
	static public <T> Facade_slice WriteFast(NovaS<T> lambda, T obj, Facade_slice S, int idx) {
		if(S.version %2 == DELETED)
			return null;
		lambda.serialize(obj,S.address+ S.offset +NovaManager.HEADER_SIZE +NovaManager.MAGIC_SIZE);
		 return S;
	}
	
	

	static public <T> T Read(NovaR<T> lambda, Facade_slice S) {
		if(S.version%2 == DELETED)
			return null;
		T obj = lambda.apply(S.address + S.offset+NovaManager.HEADER_SIZE+NovaManager.MAGIC_SIZE);
		if(bench_Flags.Fences)UNSAFE.loadFence();

		if(NovaManager.MAGIC_NUM !=  UNSAFE.getLong(S.address + S.offset) )
			return null;
		
		if(! (S.version == (int)(UNSAFE.getLong(S.address + S.offset + NovaManager.MAGIC_SIZE)&0xFFFFFF))) 
			return null;
		return obj;
	}
	 

}