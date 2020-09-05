package com.yahoo.oak;



import java.nio.ByteOrder;
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
	
	public <T> T Read(FacadeReadTransformer<T> f) {
		Triple sliceLocated=LocateSlice();

		if (sliceLocated.s == null) throw new IllegalArgumentException("cant locate slice");
		T ResultToReturn= caluclate(sliceLocated.s,f);
		Unsafe UNSAFE = UnsafeUtils.unsafe;
		UNSAFE.loadFence();

		long header= Long.reverseBytes(UNSAFE.getLong(sliceLocated.HeaderAddress));
		if(! (sliceLocated.facadeVer== (int)(header&0xFFFFFF))) 
			throw new IllegalArgumentException("slice changed");
		return ResultToReturn;

	}
	
	
	public <T> T Write(FacadeWriteTransformer<T> f) {

		long facadeRef=buildRef(block,offset);
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
//		long metadata=block_offset_ver_del.get();
//		boolean del = ExtractDel(metadata)==1 ? true: false;
	//Maybe we want this in case of two threads want to access the facade the first still in allocate and the 
	//second is in Read/Write and so the deleted didn't get updated!

		if ((version&1)==1 ) return new Triple(INVALID_VERSION,null,INVALID_HEADER);
			//delted?
		NovaSlice locatedSlice = new NovaSlice(block,offset);
		novaManager.readByteBuffer(locatedSlice);

		return  new Triple(version,locatedSlice,locatedSlice.getAddress()+offset);
	}
	
	

	
	
	
	
	
	public <T> T  caluclate(NovaSlice s ,FacadeReadTransformer<T> f) {
		 T transformation = f.apply(novaManager.getReadBuffer(s));
		 return transformation;
	}
	
	public <T> T  caluclate(NovaSlice s ,FacadeWriteTransformer<T> f) {
		 T transformation = f.apply(novaManager.getWriteBuffer(s));
		 return transformation;
	}
	

	
	public byte OnesToMask(int lenght, boolean bool){ //when @ true shift left
//		assert lenght < 8;
		byte mask= (byte)0xFF;
		return (bool? (byte) (mask<<lenght) : (byte) ((mask & 0xff)>>lenght));
	}
	
	private long buildRef(int block, int offset) {
		long Ref=(block &0xFFFFF);
		Ref=Ref<<20;
		Ref=Ref|(offset&0xFFFFF);
		return Ref;
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
	
	
	
	
	
	
	
	class XNovaReadBuffer extends NovaSlice implements OakScopedReadBuffer, OakUnsafeDirectBuffer {


		XNovaReadBuffer(NovaSlice other) {
	        super(other);
	    }

	    protected int getDataOffset(int index) {
	        if (index < 0 || index >= getLength()) {
	            throw new IndexOutOfBoundsException();
	        }
	        return getOffset() + index;
	    }

	    @Override
	    public int capacity() {
	        return getLength();
	    }

	    @Override
	    public ByteOrder order() {
	        return buffer.order();
	    }

	    @Override
	    public byte get(int index) {
	        return buffer.get(getDataOffset(index));
	    }

	    @Override
	    public char getChar(int index) {
	        return buffer.getChar(getDataOffset(index));
	    }

	    @Override
	    public short getShort(int index) {
	        return buffer.getShort(getDataOffset(index));
	    }

	    @Override
	    public int getInt(int index) {
	        return buffer.getInt(getDataOffset(index));
	    }

	    @Override
	    public long getLong(int index) {
	        return buffer.getLong(getDataOffset(index));
	    }

	    @Override
	    public float getFloat(int index) {
	        return buffer.getFloat(getDataOffset(index));
	    }

	    @Override
	    public double getDouble(int index) {
	        return buffer.getDouble(getDataOffset(index));
	    }
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
