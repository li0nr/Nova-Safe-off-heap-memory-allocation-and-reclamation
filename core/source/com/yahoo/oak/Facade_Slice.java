package com.yahoo.oak;


import sun.misc.Unsafe;

public class Facade_Slice {

	static final int INVALID_BLOCKID=0;
	static final int INVALID_OFFSET=-1;
	static final int INVALID_VERSION=0;
	static final int INVALID_HEADER=0;
	static final int DELETED=1;
			
	static NovaManager novaManager;	
	
	static final Unsafe UNSAFE=UnsafeUtils.unsafe;

	
	static {
		try {
			final Unsafe UNSAFE=UnsafeUtils.unsafe;
			Facade_slice.version_offset = UNSAFE.objectFieldOffset
				    (Facade_slice.class.getDeclaredField("version"));
			 } catch (Exception ex) { throw new Error(ex); }
	}
	
	static public class Facade_slice extends NovaSlice{

		private int version;
		static long version_offset;
		
		public Facade_slice() {
			super(INVALID_BLOCKID, INVALID_OFFSET, 0);
			version = 1;
		}
		
		public Facade_slice(Facade_slice o) {
			super(o.blockID, o.offset, o.length);
			version = o.version;
		}
		
		public boolean isDeleted() {
			if (version %2 == 1)
				return true;
			return false;
		}
		
	}
	
	public Facade_Slice(NovaManager mng) {
		novaManager = mng;
	}

	static public <K> boolean AllocateSliceCAS(Facade_slice S, int size, int idx) {
		int CurrVer = S.version;
		if(CurrVer%2 != DELETED)
			return false;
	
		NovaSlice 	newslice = novaManager.getSlice(size,idx);
		S.offset  =	newslice.offset;
		S.blockID =	newslice.blockID;
		S.address = newslice.address;
		S.length  = newslice.length;
		int NewVer= (int) newslice.getVersion() &0xFFFFFF;
		assert S.blockID != 0;
 
        return UNSAFE.compareAndSwapLong(S, Facade_slice.version_offset , CurrVer, NewVer) ?//TODO is CAS needed when we share slices?
        		true : !novaManager.free(newslice); 
	}
	
	static public <K> boolean AllocateSlice(Facade_slice S, int size, int idx) {
		int CurrVer = S.version;
		if(CurrVer%2 != DELETED)
			return false;
	
		NovaSlice 	newslice = novaManager.getSlice(size,idx);
		S.offset  =	newslice.offset;
		S.blockID =	newslice.blockID;
		S.address = newslice.address;
		S.length  = newslice.length;
		S.version = (int) newslice.getVersion() &0xFFFFFF;
		assert S.blockID != 0;
		return true;
	}
	
	
    /**
     * deletes the object referenced by the current facade 
     *
     * @param idx          the thread index that wants to delete
     */
	static public boolean DeleteCAS(int idx, Facade_slice S) {
		boolean flag = true;
		if(S.version%2 == DELETED)
			return false;
		int localBlock = S.blockID;
		int localOffset = S.offset;
		int localLenght = S.length;
		int localVer	= S.version;
		long localAddress = S.address;
		
		long SliceHeaderAddress= localAddress+localOffset;
		long OffHeapMetaData= UNSAFE.getLong(SliceHeaderAddress);//reads off heap meta

		if(!UNSAFE.compareAndSwapLong(null, SliceHeaderAddress, OffHeapMetaData,
				OffHeapMetaData|1)) //swap with CAS
			 flag = false;
		
		UNSAFE.compareAndSwapLong(S, Facade_slice.version_offset, localVer,  localVer|1);
		if(flag)
			novaManager.release(localBlock, localOffset, localLenght,idx); 
		 return flag; 
	}
	
	static public boolean Delete(int idx, Facade_slice S) {
		
		if(S.version%2 == DELETED)
			return false;		
		
		int localBlock = S.blockID;
		int localOffset = S.offset;
		int localLenght = S.length;
		long localAddress = S.address;
		
		long SliceHeaderAddress= localAddress+localOffset;

		long OffHeapMetaData= UNSAFE.getLong(SliceHeaderAddress);//reads off heap meta		


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
		novaManager.free(new NovaSlice(S.blockID, S.offset, S.length));
		return true; 
	}
	
	static public <T> Facade_slice WriteFull (NovaS<T> lambda, T obj, Facade_slice S ,int idx) {//for now write doesnt take lambda for writing 

		if(S.version%2 == DELETED)
			return null;
		
		long facadeRef	= buildRef		(S.blockID,S.offset);
		
		if(bench_Flags.TAP) {
			novaManager.setTap(facadeRef,idx);	
			if(bench_Flags.Fences)UNSAFE.fullFence();
			}
		if(! (S.version == (int)(UNSAFE.getLong(S.address+S.offset)&0xFFFFFF))) {
			novaManager.UnsetTap(idx);
			return null;
			}
		lambda.serialize(obj,S.address+ S.offset +NovaManager.HEADER_SIZE);
		 if(bench_Flags.TAP) {
             if(bench_Flags.Fences)UNSAFE.storeFence();
            novaManager.UnsetTap(idx);
            }
		 return S;
	}
	
	
	static public <T> Facade_slice WriteFast(NovaS<T> lambda, T obj, Facade_slice S, int idx) {
		if(S.version %2 == DELETED)
			return null;
		lambda.serialize(obj,S.address+ S.offset +NovaManager.HEADER_SIZE);
		 return S;
	}
	
	

	static public <T> T Read(NovaR<T> lambda, Facade_slice S) {
		if(S.version%2 == DELETED)
			return null;
		T obj = lambda.apply(S.address + S.offset+NovaManager.HEADER_SIZE);
		if(bench_Flags.Fences)UNSAFE.loadFence();
		if(! (S.version == (int)(UNSAFE.getLong(S.address + S.offset)&0xFFFFFF))) 
			return null;
		return obj;
	}

	static public <T> int Compare(T obj, NovaC<T> srZ, Facade_slice S) {
		 if(S.version%2 == DELETED)
			throw new NovaIllegalAccess();
		int res = srZ.compareKeys(S.address + S.offset +NovaManager.HEADER_SIZE, obj);
		if(bench_Flags.Fences)UNSAFE.loadFence();
		if(! (S.version == (int)(UNSAFE.getLong(S.address + S.offset)&0xFFFFFF))) 
			throw new NovaIllegalAccess();
		return res;
	}
	 
	 
	
	 
	static private long buildRef(int block, int offset) {
		long Ref=(block &0xFFFFF);
		Ref=Ref<<30;
		Ref=Ref|(offset&0xFFFFF);
		return Ref;
	}

}