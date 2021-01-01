package com.yahoo.oak;


import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

import org.openjdk.jmh.runner.RunnerException;

import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

public class Facade {

	static final int INVALID_BLOCKID=0;
	static final int INVALID_OFFSET=-1;
	static final int INVALID_VERSION=0;
	static final int INVALID_HEADER=0;
	static final int DELETED=1;
	
	static final int HeaderSize = Long.BYTES;
	
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
	
	public boolean AllocateSlice(int size, int ThreadIdx) {
		
		long data = FacadeMetaData;
		if(data%2!=DELETED)
			return false;
	
		NovaSlice 	newslice = novaManager.getSlice(size,ThreadIdx);
		int offset=	newslice.getAllocatedOffset();
		int block =	newslice.getAllocatedBlockID();
		int version=newslice.getVersion();
        
        long facadeNewData = combine(block,offset,version);

        return UNSAFE.compareAndSwapLong(this, FacadeMetaData_offset, data, facadeNewData) ?
        		true : !novaManager.free(newslice); 
	}
	
	
	public boolean Delete(int idx) {
		long facademeta=FacadeMetaData;
		
		if(facademeta %2 != 0) 
			return false;
		int block 	= Extractblock(facademeta);
		int offset	= ExtractOffset(facademeta);
		
		ByteBuffer Block=novaManager.readByteBuffer(block);
		if(Block == null)
			return false;

		long OffHeapMetaData= Block.getLong(offset);//reads off heap meta
		
		//if(off heap deleted || version is correct )//removed this
		
		long len=OffHeapMetaData>>>24; //get the lenght 
		long version = ExtractVer_Del(facademeta); //get the version in the facade including delete
		OffHeapMetaData = len <<24 | version; // created off heap style meta 

		long SliceHeaderAddress= ((DirectBuffer) Block).address() + offset;

		if(!UNSAFE.compareAndSwapLong(null, SliceHeaderAddress, Long.reverseBytes(OffHeapMetaData),
				Long.reverseBytes(OffHeapMetaData|1))) //swap with CAS
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

		
		ByteBuffer Block=novaManager.readByteBuffer(block);//try with block attached to facade

		//T R = f.apply(novaManager.getReadBuffer(sliceLocated.s));
		
		long R = Block.getLong(HeaderSize+offset);
		
		if(Flags.Fences)UNSAFE.loadFence();
		
		if(! (version == (int)(Block.getLong(offset)&0xFFFFFF))) 
			throw new IllegalArgumentException("slice changed");
		return R;
	}
	
	
	public ByteBuffer Write(long toWrite,int idx ) {//for now write doesnt take lambda for writing 

		long facademeta = FacadeMetaData;
		if(facademeta%2!=0) {
			throw new IllegalArgumentException("cant locate slice");
		}
		
		int block		= Extractblock	(facademeta);
		int offset 		= ExtractDel	(facademeta);
		long facadeRef	= buildRef		(block,offset);
		
		if(Flags.TAP) {
			novaManager.setTap(block,facadeRef,idx);	
			if(Flags.Fences)UNSAFE.fullFence();
		}
		

		ByteBuffer Block=novaManager.readByteBuffer(block);
		
		int version = ExtractVer_Del(facademeta);
		if(! (version == (int)(Block.getLong(offset)&0xFFFFFF))) {
			novaManager.UnsetTap(block,idx);
			throw new IllegalArgumentException("slice was deleted");
			}
//		T ResultToReturn= caluclate(sliceLocated.s,f);
		ByteBuffer returnBlock = Block.putLong(HeaderSize+offset, toWrite);	

		 if(Flags.TAP) {
             if(Flags.Fences)UNSAFE.storeFence();
            novaManager.UnsetTap(block,idx);
            }

		return returnBlock;
		
		
	}
	
	
	
	
	
//	public <T> T  caluclate(NovaSlice s ,FacadeReadTransformer<T> f) {
//		 T transformation = f.apply(novaManager.getReadBuffer(s));
//		 return transformation;
//	}
//	
//	public <T> T  caluclate(NovaSlice s ,FacadeWriteTransformer<T> f) {
//		 T transformation = f.apply(novaManager.getWriteBuffer(s));
//		 return transformation;
//	}
	

	
	
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
	private int ExtractVer(long toExtract) {
		int del=(int) (toExtract>>1)&0x7FFFFF;
		return del;
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
