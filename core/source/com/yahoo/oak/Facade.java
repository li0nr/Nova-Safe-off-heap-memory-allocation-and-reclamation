package com.yahoo.oak;


import sun.misc.Unsafe;

public class Facade <T> {

	static final int INVALID_BLOCKID=0;
	static final int INVALID_OFFSET=-1;
	static final int INVALID_VERSION=0;
	static final int INVALID_HEADER=0;
	static final int DELETED=1;
		
	static final long FacadeMetaData_offset;
	
	static NovaManager novaManager;	
	
	static final Unsafe UNSAFE=UnsafeUtils.unsafe;
	static {
		try {
			FacadeMetaData_offset=UNSAFE.objectFieldOffset
				    (Facade.class.getDeclaredField("FacadeMetaData"));
			 } catch (Exception ex) { throw new Error(ex); }
	}
	
	long   FacadeMetaData;

	
	public Facade(NovaManager novaManager) {
		FacadeMetaData= 1;
		this.novaManager=novaManager;
	}
	public Facade() {
		FacadeMetaData= 1;
	}
	
	public boolean AllocateSlice(int size, int idx) {
		
		long data = FacadeMetaData;
		if(data%2!=DELETED)
			return false;
	
		NovaSlice 	newslice = novaManager.getSlice(size,idx);
		int offset=	newslice.getAllocatedOffset();
		int block =	newslice.getAllocatedBlockID();
		int version=  (int)newslice.getVersion();
        
        long facadeNewData = combine(block,offset,version);

        return UNSAFE.compareAndSwapLong(this, FacadeMetaData_offset, data, facadeNewData) ?
        		true : !novaManager.free(newslice); 
	}
	
	
    /**
     * deletes the object referenced by the current facade 
     *
     * @param idx          the thread index that wants to delete
     */
	public boolean Delete(int idx) {
		long facademeta=FacadeMetaData;
		
		if(facademeta %2 != 0) 
			return false;
		int block 	= Extractblock(facademeta);
		int offset	= ExtractOffset(facademeta);
		
		//ByteBuffer Block=novaManager.readByteBuffer(block);
		long address = novaManager.getAdress(block);

		
		long OffHeapMetaData= UNSAFE.getLong(address+offset);//reads off heap meta
		
		//if(off heap deleted || version is correct )//removed this
		
		long len=OffHeapMetaData>>>24; //get the lenght 
		long version = ExtractVer_Del(facademeta); //get the version in the facade including delete
		OffHeapMetaData = len <<24 | version; // created off heap style meta 

		long SliceHeaderAddress= address + offset;

		if(!UNSAFE.compareAndSwapLong(null, SliceHeaderAddress, OffHeapMetaData,
				OffHeapMetaData|1)) //swap with CAS
			 return false;
		

		 UNSAFE.compareAndSwapLong(this, FacadeMetaData_offset, facademeta, facademeta |1);
		 
		 novaManager.release(block,offset,(int)len,idx); 
		 return true; 
	}
	
	//for now we need to Read wihtout anything
	public long Read() {
		long facademeta = FacadeMetaData;
		
		if(facademeta%2!=0)
			throw new IllegalArgumentException("cant locate slice");
		
		int version	= ExtractVer_Del(facademeta);
		int block 	= Extractblock	(facademeta);
		int offset 	= ExtractOffset	(facademeta);

		
		long address = novaManager.getAdress(block);

		//T R = f.apply(novaManager.getReadBuffer(sliceLocated.s));
		
		long R =UNSAFE.getLong(address+offset+NovaManager.HEADER_SIZE);
		
		if(bench_Flags.Fences)UNSAFE.loadFence();
		
		if(! (version == (int)(UNSAFE.getLong(address+offset)&0xFFFFFF))) 
			throw new IllegalArgumentException("slice changed");
		return R;
	}
	
	
	public Facade Write(long toWrite,int idx ) {//for now write doesnt take lambda for writing 

		long facademeta = FacadeMetaData;
		if(facademeta%2==DELETED) {
			throw new IllegalArgumentException("cant locate slice");
		}
		
		int block		= Extractblock	(facademeta);
		int offset 		= ExtractOffset	(facademeta);
		long facadeRef	= buildRef		(block,offset);
		
		if(bench_Flags.TAP) {
			novaManager.setTap(facadeRef,idx);	
			if(bench_Flags.Fences)UNSAFE.fullFence();
		}
		
		long address = novaManager.getAdress(block);

		
		int version = ExtractVer_Del(facademeta);
		if(! (version == (int)(UNSAFE.getLong(address+offset)&0xFFFFFF))) {
			novaManager.UnsetTap(idx);
			throw new IllegalArgumentException("slice was deleted");
			}
//		T ResultToReturn= caluclate(sliceLocated.s,f);
		 UNSAFE.putLong(address+NovaManager.HEADER_SIZE+offset, toWrite);	
		 if(bench_Flags.TAP) {
             if(bench_Flags.Fences)UNSAFE.storeFence();
            novaManager.UnsetTap(idx);
            }
		 return this;
	}
	
	public <T> Facade WriteFull (NovaS<T> lambda, T obj, int idx ) {//for now write doesnt take lambda for writing 

		long facademeta = FacadeMetaData;
		if(facademeta%2==DELETED) {
			throw new IllegalArgumentException("cant locate slice");
		}
		
		int block		= Extractblock	(facademeta);
		int offset 		= ExtractOffset	(facademeta);
		long facadeRef	= buildRef		(block,offset);
		
		if(bench_Flags.TAP) {
			novaManager.setTap(facadeRef,idx);	
			if(bench_Flags.Fences)UNSAFE.fullFence();
		}
		
		long address = novaManager.getAdress(block);


		
		int version = ExtractVer_Del(facademeta);
		if(! (version == (int)(UNSAFE.getLong(address+offset)&0xFFFFFF))) {
			novaManager.UnsetTap(idx);
			throw new IllegalArgumentException("slice was deleted");
			}
		lambda.serialize(obj,address+NovaManager.HEADER_SIZE+offset);
		 if(bench_Flags.TAP) {
             if(bench_Flags.Fences)UNSAFE.storeFence();
            novaManager.UnsetTap(idx);
            }
		 return this;
	}
	

	
	public <T> T Read(NovaS<T> lambda) {
		long facademeta = FacadeMetaData;
		
		if(facademeta%2!=0)
			throw new IllegalArgumentException("cant locate slice");
		
		int version	= ExtractVer_Del(facademeta);
		int block 	= Extractblock	(facademeta);
		int offset 	= ExtractOffset	(facademeta);

		
		long address = novaManager.getAdress(block);

		//T R = f.apply(novaManager.getReadBuffer(sliceLocated.s));
		
		//long R =UNSAFE.getLong(address+offset+NovaManager.HEADER_SIZE);
		long size = UNSAFE.getLong(address+offset)>>>24;//reads off heap meta

		T obj = lambda.deserialize(address+offset+NovaManager.HEADER_SIZE);
		
		if(bench_Flags.Fences)UNSAFE.loadFence();
		
		if(! (version == (int)(UNSAFE.getLong(address+offset)&0xFFFFFF))) 
			throw new IllegalArgumentException("slice changed");
		return obj;
	}

	
	public Facade WriteFast(NovaS<T> lambda, T obj, int idx ) {//for now write doesnt take lambda for writing 
		long facademeta = FacadeMetaData;
		
		if(facademeta%2!=0)
			throw new IllegalArgumentException("cant locate slice");
		
		int block 	= Extractblock	(facademeta);
		int offset 	= ExtractOffset	(facademeta);
		long address = novaManager.getAdress(block);
		lambda.serialize(obj,address+NovaManager.HEADER_SIZE+offset);

		 return this;
	}
	
	public boolean AllocateWrite_Private(NovaS<T> lambda, int size, T obj, int idx) {
		
		NovaSlice 	newslice = novaManager.getSlice(size,idx);
		int offset=	newslice.getAllocatedOffset();
		int block =	newslice.getAllocatedBlockID();
		int version=  (int)newslice.getVersion();
		long address = novaManager.getAdress(block);
		lambda.serialize(obj,address+NovaManager.HEADER_SIZE+offset);
		
        FacadeMetaData = combine(block,offset,version);
        return true; 
	}
	
	
	
	 public <T>  int Compare(T obj, NovaC<T> srZ) {
		long facademeta = FacadeMetaData;
		
		if(facademeta%2!=0)
			throw new IllegalArgumentException("cant locate slice");
		
		int version	= ExtractVer_Del(facademeta);
		int block 	= Extractblock	(facademeta);
		int offset 	= ExtractOffset	(facademeta);

		
		long address = novaManager.getAdress(block);

		//T R = f.apply(novaManager.getReadBuffer(sliceLocated.s));
		
		//long R =UNSAFE.getLong(address+offset+NovaManager.HEADER_SIZE);
		long size = UNSAFE.getLong(address+offset)>>>24;//reads off heap meta

		int res = srZ.compareKeys(address+offset+NovaManager.HEADER_SIZE, obj);
		
		if(bench_Flags.Fences)UNSAFE.loadFence();
		
		if(! (version == (int)(UNSAFE.getLong(address+offset)&0xFFFFFF))) 
			throw new IllegalArgumentException("slice changed");
		return res;	
	}
	 
	 public void Print(NovaC<T> srZ) {
		long facademeta = FacadeMetaData;
		
		
		int version	= ExtractVer_Del(facademeta);
		int block 	= Extractblock	(facademeta);
		int offset 	= ExtractOffset	(facademeta);
		
		long address = novaManager.getAdress(block);
		long size = UNSAFE.getLong(address+offset)>>>24;//reads off heap meta

		srZ.Print(address+offset+NovaManager.HEADER_SIZE);
	 
}
	 
	
	private long buildRef(int block, int offset) {
		long Ref=(block &0xFFFFF);
		Ref=Ref<<20;
		Ref=Ref|(offset&0xFFFFF);
		return Ref;
	}
	private int ExtractVer_Del(long toExtract) {
		int del=(int) (toExtract)&0x7FFFFF;
		return del;
	}
	private int ExtractOffset(long toExtract) {
		int del=(int) (toExtract>>24)&0xFFFFF;
		return del;
	}
	private int Extractblock(long toExtract) {
		int del=(int) (toExtract>>44)&0xFFFFF;
		return del;
	}
	
	private long combine(int block, int offset, int version_del ) {
		long toReturn=  (block & 0xFFFFFFFF);
		toReturn = toReturn << 20 | (offset & 0xFFFFFFFF);
		toReturn = toReturn << 24 | (version_del & 0xFFFFFFFF)  ;
		return toReturn;
	}
	private long combine( long len, int version_del ) {
		long toReturn = len;
		toReturn = toReturn << 24 | (version_del & 0xFFFFFFFF)  ;
		return toReturn;
	}

}
