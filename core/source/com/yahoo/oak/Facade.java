package com.yahoo.oak;


import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;
import  sun.misc.Unsafe;



public class Facade {

	static final int INVALID_BLOCKID=0;
	static final int INVALID_OFFSET=-1;
	static final int INVALID_VERSION=0;
	static final int INVALID_HEADER=0;
	static final boolean DELETED=true;
	
	NovaManager novaManager;
	AtomicLong   block_offset_ver_del;
	int block;
	int offset;
	int version;
	boolean deleted;

	
	public Facade(NovaManager novaManager) {
		block_offset_ver_del = new AtomicLong(1);
		this.novaManager=novaManager;
	}
	
	public boolean AllocateSlice(int size) {
		
		long data=block_offset_ver_del.get();

		boolean del = ExtractDel(data)==1 ? true: false;
		
		if (!del) throw new IllegalArgumentException("facade is occupied");	
//        novaManager.allocate(newslice, size);
//       newslice.setHeader(novaManager.getNovaEra(), size);
		NovaSlice newslice = novaManager.getSlice(size);

        offset=newslice.getAllocatedOffset();
        block=newslice.getAllocatedBlockID();
        version=newslice.getVersion();
        
        long facadeNewData = combine(block,offset,version);
        block_offset_ver_del.compareAndSet(data, facadeNewData); 
        deleted=false;
		return true;
	}
	
	public boolean Delete() {
		Triple sliceLocated=LocateSlice();
		
		if (sliceLocated.s == null) {
			throw new IllegalArgumentException("cant locate slice");
		}
		 long header= sliceLocated.s.getByteBuffer().getLong(sliceLocated.s.offset);
		 long len=header>>>24;
		 
		 
		 int Ver=sliceLocated.facadeVer;
		 long Cur=combine(len,Ver);
		 long NewV=(sliceLocated.facadeVer&0xFFFFFF)>>1;
		 NewV=NewV<<1|1&1;
		 long NewVer= (int)combine(len,(int)NewV);
		 
		Unsafe UNSAFE = UnsafeUtils.unsafe;
		long SliceHeaderAddress= sliceLocated.s.getMetadataAddress();

		if(!UNSAFE.compareAndSwapLong(null, SliceHeaderAddress, Long.reverseBytes(Cur), Long.reverseBytes(NewVer)))
			 throw new IllegalArgumentException("off-heap and slice meta dont match");


		 long FacadeVer=(sliceLocated.facadeVer&0xFFFFFF);
		
		 FacadeVer=combine(block,offset, (int) FacadeVer & 0xFFFFFF);
		 NewV     =combine(block,offset, (int) NewV & 0xFFFFFF);
		 block_offset_ver_del.compareAndSet(FacadeVer, NewV);
		 
		 novaManager.release(sliceLocated.s);
		 
		 return true; 
	}
	
	public <T> long Read(FacadeReadTransformer<T> f) {
		Triple sliceLocated=LocateSlice();

		if (sliceLocated.s == null) throw new IllegalArgumentException("cant locate slice");
		//T R = f.apply(novaManager.getReadBuffer(sliceLocated.s));
		long R = sliceLocated.s.buffer.getLong(0);
		Unsafe UNSAFE = UnsafeUtils.unsafe;
		UNSAFE.loadFence();
		if(! (sliceLocated.facadeVer== (int)(sliceLocated.Header&0xFFFFFF))) 
			throw new IllegalArgumentException("slice changed");
		return R;

	}
	
	
	public <T> ByteBuffer Write(FacadeWriteTransformer<T> f) {

		long facadeRef=buildRef(block,offset);
		novaManager.setTap(facadeRef);

		Unsafe UNSAFE = UnsafeUtils.unsafe;
		UNSAFE.fullFence();
		
		Triple sliceLocated=LocateSlice();
//		
		if (sliceLocated.s == null) {
			 novaManager.UnsetTap(facadeRef);
			throw new IllegalArgumentException("cant locate slice");
		}
		long header= LocateSlice().Header;
		 if(! (sliceLocated.facadeVer == (int)(header&0xFFFFFF))) {
			 novaManager.UnsetTap(facadeRef);
			 throw new IllegalArgumentException("slice changed");
		 }
//
//		T ResultToReturn= caluclate(sliceLocated.s,f);
		ByteBuffer ResultToReturn =sliceLocated.s.buffer.putLong(0, 2);
		novaManager.UnsetTap(facadeRef);
		
		 return ResultToReturn;
		
	}
	
	
	public Triple LocateSlice() {
//		long metadata=block_offset_ver_del.get();
//		boolean del = ExtractDel(metadata)==1 ? true: false;
	//Maybe we want this in case of two threads want to access the facade the first still in allocate and the 
	//second is in Read/Write and so the deleted didn't get updated!

		if ((version&1)==1 ) return new Triple(INVALID_VERSION,null,INVALID_HEADER);
			//delted?
		NovaSlice locatedSlice = new NovaSlice(block,offset);
		novaManager.readByteBuffer(locatedSlice);

		return  new Triple(version,locatedSlice,locatedSlice.buffer.getLong(offset));
	}
	
	

	
	
	
	
	
	public <T> T  caluclate(NovaSlice s ,FacadeReadTransformer<T> f) {
		 T transformation = f.apply(novaManager.getReadBuffer(s));
		 return transformation;
	}
	
	public <T> T  caluclate(NovaSlice s ,FacadeWriteTransformer<T> f) {
		 T transformation = f.apply(novaManager.getWriteBuffer(s));
		 return transformation;
	}
	

	
	
	private long buildRef(int block, int offset) {
		long Ref=(block &0xFFFFF);
		Ref=Ref<<20;
		Ref=Ref|(offset&0xFFFFF);
		return Ref;
	}
	
	private int ExtractDel(long toExtract) {
		int del=(int) (toExtract)&0x1;
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
	
	
	
	
	 class Triple{
		public int facadeVer;
		public NovaSlice s;
		public long Header;

		
		Triple(int ver, NovaSlice s, long header){
			facadeVer=ver;
			this.s=s;
			this.Header=header;
		}

	}
}
