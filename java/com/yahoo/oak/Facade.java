package com.yahoo.oak;



import java.util.concurrent.atomic.AtomicLong;
import  sun.misc.Unsafe;



public class Facade {

	static final int INVALID_BLOCKID=0;
	static final int INVALID_OFFSET=-1;
	static final int INVALID_VERSION=0;
	static final int INVALID_HEADER=0;
	static final int DELETED=1;
	
	NovaManager novaManager;
	AtomicLong   block_offset_ver_del;


	
	public Facade(NovaManager novaManager) {
		block_offset_ver_del = new AtomicLong(DELETED);
		this.novaManager=novaManager;
	}
	
	public boolean AllocateSlice(int size) {
		
		long data=block_offset_ver_del.get();

		boolean del = ExtractDel(data)==1 ? true: false;
		
		if (!del) throw new IllegalArgumentException("facade is occupied");
		NovaSlice newslice = new NovaSlice(INVALID_BLOCKID,INVALID_OFFSET);
		
        novaManager.allocate(newslice, size);
        
        newslice.setHeader(novaManager.getNovaEra(), size);
        int sliceOffset=newslice.getAllocatedOffset();
        
        long facadeNewData = combine(newslice.getAllocatedBlockID(),sliceOffset, (int) (newslice.buffer.getLong(sliceOffset) & 0xFFFFFF));
        block_offset_ver_del.compareAndSet(data, facadeNewData); 
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
		
		 long meta=block_offset_ver_del.get();
		 FacadeVer=combine(ExtractBlock(meta),ExtractOffset(meta), (int) FacadeVer & 0xFFFFFF);
		 NewV     =combine(ExtractBlock(meta),ExtractOffset(meta), (int) NewV & 0xFFFFFF);
		 block_offset_ver_del.compareAndSet(FacadeVer, NewV);
		 
		 novaManager.release(sliceLocated.s);
		 
		 return true; 
	}
	
	public <T> T Read(FacadeTransformer<T> f) {
		Triple sliceLocated=LocateSlice();

		if (sliceLocated.s == null) throw new IllegalArgumentException("cant locate slice");
		T ResultToReturn= caluclate(sliceLocated.s,f);
		Unsafe UNSAFE = UnsafeUtils.unsafe;
		UNSAFE.loadFence();

		long header= Long.reverseBytes(UNSAFE.getLong(sliceLocated.HeaderAddress));
		if(! (sliceLocated.facadeVer== (int)(header&0xFFFFFF))) throw new IllegalArgumentException("slice changed");
		return ResultToReturn;

	}
	
	
	public <T> T Write(FacadeWriteTransformer<T> f) {
		long data=block_offset_ver_del.get();

		long facadeRef=ExtractRef(data);
		if(!novaManager.setTap(facadeRef))
			throw new RuntimeException("Problem in TAP");

		Unsafe UNSAFE = UnsafeUtils.unsafe;
		UNSAFE.fullFence();
		
		Triple sliceLocated=LocateSlice();
		
		if (sliceLocated.s == null) {
			 novaManager.UnsetTap(facadeRef);
			throw new IllegalArgumentException("cant locate slice");
		}
		long header= Long.reverseBytes(UNSAFE.getLong(sliceLocated.HeaderAddress));
		 if(! (sliceLocated.facadeVer == (int)(header&0xFFFFFF))) {
			 novaManager.UnsetTap(facadeRef);
			 throw new IllegalArgumentException("slice changed");
		 }

		T ResultToReturn= caluclate(sliceLocated.s,f);
		 if(!novaManager.UnsetTap(facadeRef))
				throw new RuntimeException("Problem in TAP");

		 return ResultToReturn;
		
	}
	
	
	public Triple LocateSlice() {
		long metadata=block_offset_ver_del.get();
		
		boolean del = ExtractDel(metadata)==1 ? true: false;
		

		if (del ) return new Triple(INVALID_VERSION,null,INVALID_HEADER);

		
		int block  = ExtractBlock(metadata);
		int offset = ExtractOffset(metadata);
		
		NovaSlice locatedSlice = new NovaSlice(block,offset);
		novaManager.readByteBuffer(locatedSlice);

		return  new Triple((int)(metadata&0xFFFFFF),locatedSlice,locatedSlice.getAddress()+offset);
	}
	
	

	
	
	
	
	
	public <T> T  caluclate(NovaSlice s ,FacadeTransformer<T> f) {
		 T transformation = f.apply(new NovaReadBuffer(s));
		 return transformation;
	}
	
	public <T> T  caluclate(NovaSlice s ,FacadeWriteTransformer<T> f) {
		 T transformation = f.apply(new NovaWriteBuffer(s));
		 return transformation;
	}
	
//	public <T> T  caluclate(FacadeWriteTransformer<T> f, int block, int offset) {
//		novaManager.readByteBuffer(block);
//		
//		 long header= novaManager.readByteBuffer(block).getLong(offset);
//		 long len=header>>>24;
//		 
////	        if (index < 0 || index >= len) {
////	            throw new IndexOutOfBoundsException();
////	        }
//		 T transformation = f.apply(new NovaWriteBuffer(s));
//		 return transformation;
//	}
	
	//	int leftOverByte = offset%8;
	//	int ByteToRead =  offset /8;
	//	byte mask=OnesToMask(leftOverByte, false);

	//	byte[] bytes = Longs.toByteArray((long) buff.getLong(ByteToRead));
	//	Integer masked= mask & bytes[0];
	//	bytes[0]= masked.byteValue();
//		long len =buff.getLong(ByteToRead) >>>24;
//		long datalen =len-headerSize;
		
//		int dataOffet=offset+headerSize;
	//	int BytesToRead=0;

//		if((((int)datalen+offset)%8!=0)) {
//			BytesToRead=((int)datalen+offset)/8 +1;
//		}else BytesToRead=((int)datalen+offset)/8;
//		
//		byte[] dataToRead= new byte[BytesToRead];// TODO Ramy be carefull data len is long
//		int ReadData=0;
//		int ReadIndex=dataOffet/8;
//		int i=0;
//		while(ReadData<datalen) {
//			dataToRead[i]=buff.get(ReadIndex);
//			if(i==0 && dataOffet%8 !=0) {
//				Integer dataMask=dataToRead[i] & OnesToMask(dataOffet%8, false);
//				dataToRead[0]=dataMask.byteValue();
//				ReadData+=8-dataOffet%8;
//			}else {
//				ReadData+=8;
//			}
//			
//			i++;
//			ReadIndex++;
//		}
//		if (((int)datalen+offset)%8!=0) {
//			shiftRight(dataToRead,8-((int)datalen+offset)%8);
//		}
//	}

	
	public byte OnesToMask(int lenght, boolean bool){ //when @ true shift left
//		assert lenght < 8;
		byte mask= (byte)0xFF;
		return (bool? (byte) (mask<<lenght) : (byte) ((mask & 0xff)>>lenght));
	}
	
	private SimplePair extractRef(long toExtract) {
		int block=(int) (toExtract >>44 );
		int offset= (int) (toExtract >>24)&0xFFFF;
		return new SimplePair(block,offset);
	}
	
	private SimplePair extractversion(long toExtract) {
		int version=(int) (toExtract >>1 )&0x7FFFF;
		int del= (int) (toExtract)& 0x1;
		return new SimplePair(version,del);
	}
	
	private long ExtractRef(long toExtract) {
		return (long) (toExtract >>24 );

	}
	private int ExtractVersion(long toExtract) {
		int version=(int) (toExtract >>1 )&0x7FFFF;
		return version;
	}
	private int ExtractDel(long toExtract) {
		int del=(int) (toExtract)&0x1;
		return del;
	}
	private int ExtractBlock(long toExtract) {
		return (int) (toExtract >>44 );
	}
	private int ExtractOffset(long toExtract) {
		return  (int) (toExtract >>24)&0xFFFFF;	
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
	
	
	
	
	
	
	
	
    /**TODO RAMY taken from https://github.com/patrickfav/bytes-java/blob/master/src/main/java/at/favre/lib/bytes/Util.java
     * Unsigned/logical right shift of whole byte array by shiftBitCount bits.
     * This method will alter the input byte array.
     *
     * <p>
     * <strong>Analysis</strong>
     * <ul>
     * <li>Time Complexity: <code>O(n)</code></li>
     * <li>Space Complexity: <code>O(1)</code></li>
     * <li>Alters Parameters: <code>true</code></li>
     * </ul>
     * </p>
     *
     * @param byteArray     to shift
     * @param shiftBitCount how many bits to shift
     * @return shifted byte array
     */
    static byte[] shiftRight(byte[] byteArray, int shiftBitCount) {
        final int shiftMod = shiftBitCount % 8;
        final byte carryMask = (byte) (0xFF << (8 - shiftMod));
        final int offsetBytes = (shiftBitCount / 8);

        int sourceIndex;
        for (int i = byteArray.length - 1; i >= 0; i--) {
            sourceIndex = i - offsetBytes;
            if (sourceIndex < 0) {
                byteArray[i] = 0;
            } else {
                byte src = byteArray[sourceIndex];
                byte dst = (byte) ((0xff & src) >>> shiftMod);
                if (sourceIndex - 1 >= 0) {
                    dst |= byteArray[sourceIndex - 1] << (8 - shiftMod) & carryMask;
                }
                byteArray[i] = dst;
            }
        }
        return byteArray;
    }
	
	
	
	 class Triple{
		public int facadeVer;
		public NovaSlice s;
		public long HeaderAddress;

		
		Triple(int ver, NovaSlice s, long header){
			facadeVer=ver;
			this.s=s;
			this.HeaderAddress=header;
		}

	}
}
