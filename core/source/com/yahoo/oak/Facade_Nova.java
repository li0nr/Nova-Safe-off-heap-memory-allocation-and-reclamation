package com.yahoo.oak;


import java.util.function.BiFunction;
import java.util.function.Function;

import com.yahoo.oak.Facade_Slice.Facade_slice;

import sun.misc.Unsafe;

public class Facade_Nova {

	static final int INVALID_BLOCKID=0;
	static final int INVALID_OFFSET=-1;
	static final int INVALID_VERSION=0;
	static final int INVALID_HEADER=0;
	static final int DELETED=1;
			
	static NovaManager novaManager;	
	
	static final Unsafe UNSAFE=UnsafeUtils.unsafe;

	public Facade_Nova(NovaManager mng) {
		novaManager = mng;
	}

	static public <K> long AllocateReusedSlice(K obj, long meta_offset, long data, int size, int idx) {
		if(data%2!=DELETED)
			return -1;
		NovaSlice 	newslice = novaManager.getSlice(size,idx);
		int offset=	newslice.getAllocatedOffset();
		int block =	newslice.getAllocatedBlockID();
		int version=  (int)newslice.getVersion();
        long facadeNewData = _Global_Defs.combine(block,offset,version);
        if(!UNSAFE.compareAndSwapLong(obj, meta_offset, data, facadeNewData))
        	novaManager.free(newslice);
        return facadeNewData;
	}
	
	static public <K> long AllocateSlice(int size, int idx) {
		NovaSlice 	newslice = novaManager.getSlice(size,idx);
		int offset=	newslice.getAllocatedOffset();
		int block =	newslice.getAllocatedBlockID();
		int version=  (int)newslice.getVersion();
        long facadeNewData = _Global_Defs.combine(block,offset,version);
        return facadeNewData;
	}
	
	
    /**
     * deletes the object referenced by the current facade 
     *
     * @param  idx the thread index that wants to delete
     */
	static public <K> boolean DeleteReusedSlice(int idx, long metadata, K obj, long meta_offset) {
		boolean flag = true;
		if(metadata %2 != 0) 
			return false;
		int block 	= _Global_Defs.Extractblock(metadata);
		int offset	= _Global_Defs.ExtractOffset(metadata);
		
		long address = novaManager.getAdress(block);
		long OffHeapMetaData= UNSAFE.getLong(address+offset);//reads off heap meta
		
		long len=OffHeapMetaData>>>24; //get the lenght 
		long version = _Global_Defs.ExtractVer_Del(metadata); //get the version in the facade including delete
		OffHeapMetaData = len <<24 | version; // created off heap style meta 

		long SliceHeaderAddress= address + offset;

		if(!UNSAFE.compareAndSwapLong(null, SliceHeaderAddress, OffHeapMetaData,
				OffHeapMetaData|1)) //swap with CAS
			 flag = false;
		 UNSAFE.compareAndSwapLong(obj, meta_offset, metadata, metadata |1);
		 if(flag)
			 novaManager.release(block,offset,(int)len,idx); 
		 return flag; 
	}

	static public <K> boolean Delete(int idx, long metadata ) {
		if(metadata %2 != 0) 
			return false;
		int block 	= _Global_Defs.Extractblock(metadata);
		int offset	= _Global_Defs.ExtractOffset(metadata);
		long address = novaManager.getAdress(block);

		long OffHeapMetaData= UNSAFE.getLong(address+offset);//reads off heap meta
				
		long SliceHeaderAddress= address + offset;

		if(!UNSAFE.compareAndSwapLong(null, SliceHeaderAddress, OffHeapMetaData,
				OffHeapMetaData|1)) //swap with CAS
			 return false;

		long len=OffHeapMetaData>>>24; //get the length
		 novaManager.release(block,offset,(int)len,idx); 
		 return true; 
	}

	
	static public <K> boolean DeletePrivate(int idx, long metadata) {
		if(metadata %2 != 0) 
			return false;
		int block 	= _Global_Defs.Extractblock(metadata);
		int offset	= _Global_Defs.ExtractOffset(metadata);
		long address = novaManager.getAdress(block);

		long OffHeapMetaData= UNSAFE.getLong(address+offset);//reads off heap meta
		int len= (int)OffHeapMetaData>>>24; //get the lenght 

		 
		novaManager.free(new NovaSlice(block, offset, len));
		return true; 
	}
	
	static public <T> long WriteFull (NovaS<T> lambda, T obj, long facade_meta ,int idx ) {//for now write doesnt take lambda for writing 

		if(facade_meta%2==DELETED) 
			return -1;
		
		int block		= _Global_Defs.Extractblock	(facade_meta);
		int offset 		= _Global_Defs.ExtractOffset	(facade_meta);
		long facadeRef	= _Global_Defs.buildRef		(block,offset);
		
		if(bench_Flags.TAP) {
			novaManager.setTap(facadeRef,idx);	
			if(bench_Flags.Fences)UNSAFE.fullFence();
		}
		
		long address = novaManager.getAdress(block);

		int version = _Global_Defs.ExtractVer_Del(facade_meta);
		if(! (version == (int)(UNSAFE.getLong(address+offset)&0xFFFFFF))) {
			novaManager.UnsetTap(idx);
			return -1;
			}
		lambda.serialize(obj,address+NovaManager.HEADER_SIZE+offset);
		 if(bench_Flags.TAP) {
			 if(bench_Flags.Fences)UNSAFE.storeFence();
			 novaManager.UnsetTap(idx);
			 }
		 return facade_meta;
	}
	
	static public <T> long OverWrite (Function<Long,Long> lambda, long facade_meta,int idx) {//for now write doesnt take lambda for writing 

		if(facade_meta%2==DELETED) 
			return -1;
		
		int block		= _Global_Defs.Extractblock	(facade_meta);
		int offset 		= _Global_Defs.ExtractOffset	(facade_meta);
		long facadeRef	= _Global_Defs.buildRef		(block,offset);
		
		if(bench_Flags.TAP) {
			novaManager.setTap(facadeRef,idx);	
			if(bench_Flags.Fences)UNSAFE.fullFence();
		}
		
		long address = novaManager.getAdress(block);

		int version = _Global_Defs.ExtractVer_Del(facade_meta);
		if(! (version == (int)(UNSAFE.getLong(address+offset)&0xFFFFFF))) {
			novaManager.UnsetTap(idx);
			return -1;
			}
		lambda.apply(address+offset+NovaManager.HEADER_SIZE);
		//lambda.serialize(obj,address+NovaManager.HEADER_SIZE+offset);
		 if(bench_Flags.TAP) {
			 if(bench_Flags.Fences)UNSAFE.storeFence();
			 novaManager.UnsetTap(idx);
			 }
		 return facade_meta;
	}
	
	
	static public <T> T Read(NovaR<T> lambda, long metadata) {
	
		if(metadata%2!=0)
			return null;
		int version	= _Global_Defs.ExtractVer_Del(metadata);
		int block 	= _Global_Defs.Extractblock	(metadata);
		int offset 	= _Global_Defs.ExtractOffset	(metadata);

		
		long address = novaManager.getAdress(block);

		T obj = lambda.apply(address+offset+NovaManager.HEADER_SIZE);
		
		if(bench_Flags.Fences)UNSAFE.loadFence();
		
		if(! (version == (int)(UNSAFE.getLong(address+offset)&0xFFFFFF))) 
			return null;
		return obj;
	}

	
	static public <T>long WriteFast(NovaS<T> lambda, T obj, long facade_meta, int idx ) {

		if(facade_meta%2!=0)
			return -1;
		int block 	= _Global_Defs.Extractblock	(facade_meta);
		int offset 	= _Global_Defs.ExtractOffset	(facade_meta);
		long address = novaManager.getAdress(block);
		lambda.serialize(obj,address+NovaManager.HEADER_SIZE+offset);
		return facade_meta;
	}
	
	
	 static public <T> int Compare(T obj, NovaC<T> srZ, long metadata) {
		
		if(metadata%2!=0)
			throw new NovaIllegalAccess();
		
		int version	= _Global_Defs.ExtractVer_Del(metadata);
		int block 	= _Global_Defs.Extractblock	(metadata);
		int offset 	= _Global_Defs.ExtractOffset	(metadata);

		
		long address = novaManager.getAdress(block);

		int res = srZ.compareKeys(address+offset+NovaManager.HEADER_SIZE, obj);
		
		if(bench_Flags.Fences)UNSAFE.loadFence();
		if(! (version == (int)(UNSAFE.getLong(address+offset)&0xFFFFFF))) 
			throw new NovaIllegalAccess();
		return res;	
	}
	 
	 static public <T> int Compare(NovaC<T> srZ, long metadata1, long metadata2) {
			
		if(metadata1%2!=0 ||metadata2%2!=0 )
			throw new NovaIllegalAccess();
		
		int version	= _Global_Defs.ExtractVer_Del(metadata1);
		int block 	= _Global_Defs.Extractblock	(metadata1);
		int offset 	= _Global_Defs.ExtractOffset	(metadata1);
		
		int version2	= _Global_Defs.ExtractVer_Del(metadata2);
		int block2 		= _Global_Defs.Extractblock	(metadata2);
		int offset2 	= _Global_Defs.ExtractOffset	(metadata2);

		
		long address = novaManager.getAdress(block);
		long address2 = novaManager.getAdress(block2);


		int res = srZ.compareKeys(address+offset+NovaManager.HEADER_SIZE, address2+offset2+NovaManager.HEADER_SIZE);
		
		if(bench_Flags.Fences)UNSAFE.loadFence();

		if( (version != (int)(UNSAFE.getLong(address+offset)&0xFFFFFF) ||
				 (version2 != (int)(UNSAFE.getLong(address2+offset2)&0xFFFFFF) ))) 
			throw new NovaIllegalAccess();
		return res;	
	}
	 
	 
	 
	 static public <T> void Print(NovaC<T> srZ, long facademeta) {
			
			int block 	= _Global_Defs.Extractblock	(facademeta);
			int offset 	= _Global_Defs.ExtractOffset	(facademeta);
			
			long address = novaManager.getAdress(block);

			srZ.Print(address+offset+NovaManager.HEADER_SIZE);
		 
	 }
	
	
	
	//Used in BST 
	static public <T>long ReadFromOffheap(NovaS<T> lambda, long source_meta, long facade_meta, int idx ) {//for now write doesnt take lambda for writing 

		if(facade_meta%2!=0 || source_meta%2 != 0)
			throw new NovaIllegalAccess();
		
		int block 	= _Global_Defs.Extractblock	(facade_meta);
		int offset 	= _Global_Defs.ExtractOffset	(facade_meta);
		long address = novaManager.getAdress(block);
		
		int version = _Global_Defs.ExtractVer_Del(source_meta);
		int block2 	= _Global_Defs.Extractblock	(source_meta);
		int offset2 	= _Global_Defs.ExtractOffset	(source_meta);
		long address2 = novaManager.getAdress(block2);
		
		lambda.serialize(address2+NovaManager.HEADER_SIZE+offset2,
				address+NovaManager.HEADER_SIZE+offset);

		if(bench_Flags.Fences)UNSAFE.loadFence();
		
		if(! (version == (int)(UNSAFE.getLong(address2+offset2)&0xFFFFFF))) 
			throw new NovaIllegalAccess();
		
		 return facade_meta;
	}

}
