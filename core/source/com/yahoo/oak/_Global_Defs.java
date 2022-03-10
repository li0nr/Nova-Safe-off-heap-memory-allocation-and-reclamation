package com.yahoo.oak;

public class _Global_Defs {

    public static int CACHE_PADDING = 8;
	public static int MAX_THREADS = 32;
	public static long MAXSIZE = ((long) Integer.MAX_VALUE) * 16;
    public static int RELEASE_LIST_LIMIT = 1024;
    
	static  long buildRef(int block, int offset) {
		long Ref=(block &0xFFFFF);
		Ref=Ref<<30; //TODO bigger block bigger offset 
		Ref=Ref|(offset& 0x3FFFFFFF);
		assert Ref != 0;
		return Ref;
	}
	static int ExtractVer_Del(long toExtract) {
		int del=(int) (toExtract)&0xFFFFFF; //TODO 
		return del;
	}
	static int ExtractOffset(long toExtract) {
		int del=(int) (toExtract>>24)&0x3FFFFFFF; //TODO bigger block bigger offset 
		return del;
	}
	static int Extractblock(long toExtract) {
		int del=(int) (toExtract>>54)&0x3FF; //bigger block smaller block number
		return del;
	}
	static long combine(int block, int offset, int version_del ) {
		long toReturn=  (block & 0xFFFFFFFF);
		toReturn = toReturn << 30 | (offset & 0x3FFFFFFF);//TODO bigger block bigger offset 
		toReturn = toReturn << 24 | (version_del & 0xFFFFFFFF)  ;
		return toReturn;
	}
}