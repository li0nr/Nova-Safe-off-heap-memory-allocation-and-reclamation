package com.yahoo.oak;


import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

public class Facade {

	static final int INVALID_BLOCKID=0;
	static final int INVALID_OFFSET=-1;
	static final int INVALID_VERSION=0;
	static final int INVALID_HEADER=0;
	static final boolean DELETED=true;
	static final long valueOffset;
	static final Unsafe UNSAFE=UnsafeUtils.unsafe;
	static {
		try {
			 valueOffset=UNSAFE.objectFieldOffset
				    (Long.class.getDeclaredField("value"));
			 } catch (Exception ex) { throw new Error(ex); }
	}

	
	
	static final int HeaderSize = Long.BYTES;
	NovaManager novaManager;
	ByteBuffer Block;
	
	Long   block_offset_ver_del_;

	
	public Facade(NovaManager novaManager) {
		block_offset_ver_del_= new Long(1);
		this.novaManager=novaManager;
	}
	
	public boolean AllocateSlice(int size) {
		
		long data = block_offset_ver_del_;
		boolean del = ExtractDel(block_offset_ver_del_)==1 ? true: false;
		
		if (!del) 
			throw new IllegalArgumentException("facade is occupied");	
//       novaManager.allocate(newslice, size);
//       newslice.setHeader(novaManager.getNovaEra(), size);
		NovaSlice newslice = novaManager.getSlice(size); //FIXME

         int offset=newslice.getAllocatedOffset();
         int block=newslice.getAllocatedBlockID();
         int version=newslice.getVersion();
        
        long facadeNewData = combine(block,offset,version);

        return UNSAFE.compareAndSwapLong(block_offset_ver_del_, valueOffset, data, facadeNewData);

	}
	
	public boolean Delete() {
		int del = ExtractDel(block_offset_ver_del_);		
		if (del==1 ) {
			throw new IllegalArgumentException("cant locate slice");
		}
		int block = Extractblock(block_offset_ver_del_);
		novaManager.readByteBuffer(this,block);
		int version=ExtractVer_Del(block_offset_ver_del_);
		int offset = ExtractOffset(block_offset_ver_del_);
		
		long header= Block.getLong(offset); 
		long len=header>>>24;//check for errors here since i think len is always int (release)
		 
//		 //CHECK (confuse)
//		 long Cur=combine(len,version); 
//		 long NewV=(version&0xFFFFFF)>>1;
//		 NewV=NewV<<1|1&1;
//		 long NewVer= (int)combine(len,(int)NewV);
		 long NewVer = header+1;
		 long SliceHeaderAddress= ((DirectBuffer) Block).address() + offset;

		if(!UNSAFE.compareAndSwapLong(null, SliceHeaderAddress, Long.reverseBytes(header), Long.reverseBytes(NewVer)))
			 throw new IllegalArgumentException("off-heap and slice meta dont match");
		
		long FacadeVer=(version&0xFFFFFF);
		FacadeVer=combine(block,offset, (int) FacadeVer & 0xFFFFFF);
		 //NewV     =combine(block,offset, (int) NewV & 0xFFFFFF);
		 NewVer = FacadeVer+1;
		 UNSAFE.compareAndSwapLong(block_offset_ver_del_, valueOffset, FacadeVer, NewVer);
		 novaManager.release(new NovaSlice(block,offset,(int)len)); 
		 return true; 
	}
	
	public <T> long Read(FacadeReadTransformer<T> f) {
		int del = ExtractDel(block_offset_ver_del_);
		int version = ExtractVer_Del(block_offset_ver_del_);
		int block = Extractblock(block_offset_ver_del_);
		int offset = ExtractOffset(block_offset_ver_del_);
		if (del==1) 
			throw new IllegalArgumentException("cant locate slice");
		Block=novaManager.allocator.readByteBuffer(block);
		//novaManager.readByteBuffer(this);//***locate slice**
		//T R = f.apply(novaManager.getReadBuffer(sliceLocated.s));
		long R = Block.getLong(HeaderSize+offset);
		UNSAFE.loadFence();
		if(! (version == (int)(Block.getLong(offset)&0xFFFFFF))) 
			throw new IllegalArgumentException("slice changed");
		return R;

	}
	
	
	public <T> ByteBuffer Write(FacadeWriteTransformer<T> f) {
		int block = Extractblock(block_offset_ver_del_);
		int offset = ExtractDel(block_offset_ver_del_);
		long facadeRef=buildRef(block,offset);
		novaManager.setTap(block,facadeRef);
		
		UNSAFE.fullFence();
		
		int del = ExtractDel(block_offset_ver_del_);
		if (del==1 ) {
			novaManager.UnsetTap(block,facadeRef);
			throw new IllegalArgumentException("cant locate slice");
		}

		Block=novaManager.allocator.readByteBuffer(block);
		int version = ExtractVer_Del(block_offset_ver_del_);
		 if(! (version == (int)(Block.getLong(offset)&0xFFFFFF))) {
			 novaManager.UnsetTap(block,facadeRef);
			 throw new IllegalArgumentException("slice changed");
		 }
		 
//		T ResultToReturn= caluclate(sliceLocated.s,f);
		ByteBuffer ResultToReturn = Block.putLong(HeaderSize+offset, 3);
		novaManager.UnsetTap(block,facadeRef);
		
		 return ResultToReturn;
		
	}
	
	
//	public boolean LocateSlice(int block) {
//		if ((version&1)==1 ) return false;
//		novaManager.readByteBuffer(this,block);
//		return  true;
//	}
	
	

	
	
	
	
	
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
