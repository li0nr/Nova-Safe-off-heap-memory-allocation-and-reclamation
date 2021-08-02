package com.yahoo.oak;

public class NovaSliceDefrag extends NovaSlice {
	
	public NovaSliceDefrag(NovaSlice o){
		super(o);
	}
	
	public int getOffsetwithLenght() {
		return offset+length;
	}
	
	public boolean CheckForMatch(NovaSlice o) {
		if(this.blockID != o.blockID)
			return false;
		return this.offset+this.length == o.offset;
	}
	
    void setLenght(int length) {
    	this.length = length;
    }
    
    @Override
    public int compareTo(NovaSlice o) {
        int cmp = Integer.compare(this.blockID, o.blockID);
        if (cmp != 0) {
            return cmp;
        }
        return Integer.compare(this.offset, o.offset);
   }

}