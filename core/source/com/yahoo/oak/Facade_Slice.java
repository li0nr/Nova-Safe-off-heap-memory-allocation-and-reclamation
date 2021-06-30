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
		return true;
	}
	
	
    /**
     * deletes the object referenced by the current facade 
     *
     * @param idx          the thread index that wants to delete
     */
	static public boolean DeleteCAS(int idx, Facade_slice S) {
	
		if(S.version%2 == DELETED)
			throw new NovaIllegalAccess();
		
		
		long OffHeapMetaData= UNSAFE.getLong(S.address+S.offset);//reads off heap meta
		
		
		OffHeapMetaData = S.length <<24 | S.version; // created off heap style meta 

		long SliceHeaderAddress= S.address + S.offset;

		if(!UNSAFE.compareAndSwapLong(null, SliceHeaderAddress, OffHeapMetaData,
				OffHeapMetaData|1)) //swap with CAS
			 return false;
		
		int CurrVer = S.version;
		 UNSAFE.compareAndSwapLong(S, Facade_slice.version_offset, CurrVer,  CurrVer|1);
		 
		 novaManager.release(S.blockID, S.offset, S.length,idx); 
		 return true; 
	}
	
	static public boolean Delete(int idx, Facade_slice S) {
		
		if(S.version%2 == DELETED)
			throw new NovaIllegalAccess();
		
		
		long OffHeapMetaData= UNSAFE.getLong(S.address+S.offset);//reads off heap meta
		
		
		OffHeapMetaData = S.length <<24 | S.version; // created off heap style meta 

		long SliceHeaderAddress= S.address + S.offset;

		if(!UNSAFE.compareAndSwapLong(null, SliceHeaderAddress, OffHeapMetaData,
				OffHeapMetaData|1)) //swap with CAS
			 return false;
				 
		 novaManager.release(S.blockID, S.offset, S.length,idx); 
		 return true; 
	}


	static public <K> boolean DeletePrivate(int idx, Facade_slice S) {
		
		if(S.version%2 == DELETED)
			throw new NovaIllegalAccess();
		
	
		long OffHeapMetaData= UNSAFE.getLong(S.address+S.offset);//reads off heap meta
		 
		novaManager.free(new NovaSlice(S.blockID, S.offset, S.length));
		return true; 
	}
	
	static public <T> Facade_slice WriteFull (NovaS<T> lambda, T obj, Facade_slice S ,int idx) {//for now write doesnt take lambda for writing 

		if(S.version%2 == DELETED)
			throw new NovaIllegalAccess();
		
		long facadeRef	= buildRef		(S.blockID,S.offset);
		
		if(bench_Flags.TAP) {
			novaManager.setTap(S.blockID,facadeRef,idx);	
			if(bench_Flags.Fences)UNSAFE.fullFence();
		}
				
		if(! (S.version == (int)(UNSAFE.getLong(S.address+S.offset)&0xFFFFFF))) {
			novaManager.UnsetTap(S.blockID,idx);
			throw new NovaIllegalAccess();
			}
		lambda.serialize(obj,S.address+ S.offset +NovaManager.HEADER_SIZE);
		 if(bench_Flags.TAP) {
             if(bench_Flags.Fences)UNSAFE.storeFence();
            novaManager.UnsetTap(S.blockID,idx);
            }
		 return S;
	}
	
	
	static public <T> Facade_slice WriteFast(NovaS<T> lambda, T obj, Facade_slice S, int idx) {//for now write doesnt take lambda for writing 
	
		if(S.version %2 == DELETED)
			throw new NovaIllegalAccess();

		lambda.serialize(obj,S.address+ S.offset +NovaManager.HEADER_SIZE);
		 return S;
	}
	
	

	static public <T> T Read(NovaS<T> lambda, Facade_slice S) {
	
		if(S.version%2 == DELETED)
			throw new NovaIllegalAccess();

		T obj = lambda.deserialize(S.address + S.offset+NovaManager.HEADER_SIZE);
		
		if(bench_Flags.Fences)UNSAFE.loadFence();
		
		if(! (S.version == (int)(UNSAFE.getLong(S.address + S.offset)&0xFFFFFF))) 
			throw new NovaIllegalAccess();
		return obj;
	}


	
	static public <T> int Compare(T obj, NovaC<T> srZ, Facade_slice S) {
		 if(S.version%2 == DELETED)
			throw new NovaIllegalAccess();

		int res = srZ.compareKeys(S.address + S.offset +NovaManager.HEADER_SIZE, obj);
		
		if(bench_Flags.Fences)UNSAFE.loadFence();
		
		if(S.version >> 1 == novaManager.getCurrentVersion())
			return res;
		if(! (S.version == (int)(UNSAFE.getLong(S.address + S.offset)&0xFFFFFF))) 
			throw new NovaIllegalAccess();
		return res;
	}
	 
	 
	
	 
	static private long buildRef(int block, int offset) {
		long Ref=(block &0xFFFFF);
		Ref=Ref<<20;
		Ref=Ref|(offset&0xFFFFF);
		return Ref;
	}

}