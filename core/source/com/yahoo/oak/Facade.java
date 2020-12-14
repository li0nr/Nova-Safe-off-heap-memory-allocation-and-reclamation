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
	
	static final long valueOffset;
	
	static NovaManager novaManager;	
	
	
	static final Unsafe UNSAFE=UnsafeUtils.unsafe;
	static {
		try {
			 valueOffset=UNSAFE.objectFieldOffset
				    (Facade.class.getDeclaredField("block_offset_ver_del_"));
			 } catch (Exception ex) { throw new Error(ex); }
	}
	
	long   block_offset_ver_del_;

	
	public Facade(NovaManager novaManager) {
		block_offset_ver_del_= 1;
		this.novaManager=novaManager;
	}
	
	public boolean AllocateSlice(int size, int ThreadIdx) {
		
		long data = block_offset_ver_del_;
		if(block_offset_ver_del_%2!=DELETED)//FIXME add note why you did this
			return false;
	
		NovaSlice 	newslice = novaManager.getSlice(size,ThreadIdx);
		int offset=	newslice.getAllocatedOffset();
		int block =	newslice.getAllocatedBlockID();
		int version=newslice.getVersion();
        
        long facadeNewData = combine(block,offset,version);

        return UNSAFE.compareAndSwapLong(this, valueOffset, data, facadeNewData);//FIXME need to free slice?
        //add if condition if CAS fails
	}
	
	public boolean Delete(int idx) {
		long facadeHeader=block_offset_ver_del_;
		
		int block 	= Extractblock(facadeHeader);
		int offset	= ExtractOffset(facadeHeader);
		ByteBuffer Block=novaManager.readByteBuffer(block);//do we want to add this to every read write?
		if(Block == null)
			return false;
			//throw new RunnerException("facade not constucted yet");
		long OffHeapMetaData= Block.getLong(offset); //if facade data is deleted header has a deleted version as well

		if(block_offset_ver_del_%2!=0)
			return false;

		//if off heap needs to be deleted
		long len=OffHeapMetaData>>>24;// FIXME check for errors here since i think len is always int (release)

		 long NewVer = OffHeapMetaData |1;
		 long SliceHeaderAddress= ((DirectBuffer) Block).address() + offset;

		if(!UNSAFE.compareAndSwapLong(null, SliceHeaderAddress, Long.reverseBytes(OffHeapMetaData), Long.reverseBytes(NewVer)))
			 return false;
		
		 NewVer = facadeHeader |1;
		 UNSAFE.compareAndSwapLong(this, valueOffset, facadeHeader, NewVer);
		 
		 novaManager.release(block,offset,(int)len,idx); 
		 return true; 
	}
	
	//for now we need to Read wihtout anything
	public <T> long Read(FacadeReadTransformer<T> f) {
		int version	= ExtractVer_Del(block_offset_ver_del_);
		int block 	= Extractblock	(block_offset_ver_del_);
		int offset 	= ExtractOffset	(block_offset_ver_del_);
		if(block_offset_ver_del_%2!=0)
			throw new IllegalArgumentException("cant locate slice");
		
		ByteBuffer Block=novaManager.readByteBuffer(block);

		//T R = f.apply(novaManager.getReadBuffer(sliceLocated.s));
		
		long R = Block.getLong(HeaderSize+offset);
		
		UNSAFE.loadFence();
		if(! (version == (int)(Block.getLong(offset)&0xFFFFFF))) 
			throw new IllegalArgumentException("slice changed");
		return R;
	}
	
	
	public <T> ByteBuffer Write(FacadeWriteTransformer<T> f,int idx) {
		int block	= Extractblock(block_offset_ver_del_);
		int offset 	= ExtractDel(block_offset_ver_del_);
		long facadeRef=buildRef(block,offset);
		
		novaManager.setTap(block,facadeRef,idx);
		
		UNSAFE.fullFence();
		
		if(block_offset_ver_del_%2!=0) {
			novaManager.UnsetTap(block,facadeRef,idx);
			throw new IllegalArgumentException("cant locate slice");
		}

		ByteBuffer Block=novaManager.readByteBuffer(block);
		
		int version = ExtractVer_Del(block_offset_ver_del_);
		 if(! (version == (int)(Block.getLong(offset)&0xFFFFFF))) {
			 novaManager.UnsetTap(block,facadeRef,idx);
			 throw new IllegalArgumentException("slice changed");
		 }
		 
//		T ResultToReturn= caluclate(sliceLocated.s,f);
		ByteBuffer ResultToReturn = Block.putLong(HeaderSize+offset, 3);
		novaManager.UnsetTap(block,facadeRef,idx);
		
		 return ResultToReturn;
		
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
