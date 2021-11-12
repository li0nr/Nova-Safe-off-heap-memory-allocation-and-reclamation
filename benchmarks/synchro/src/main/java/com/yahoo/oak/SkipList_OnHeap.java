package com.yahoo.oak;

import com.yahoo.oak.Buff.Buff;
import com.yahoo.oak.synchrobench.contention.abstractions.CompositionalLL;

import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ConcurrentSkipListMap;


public class SkipList_OnHeap implements CompositionalLL<Buff,Buff> {
    private ConcurrentSkipListMap<Buff, Buff> skipListMap;

    public SkipList_OnHeap() {
        skipListMap = new ConcurrentSkipListMap<>();
    }

	@Override
	public Integer containsKey(final Buff key, int tidx) {
		Buff value = skipListMap.get(key);
		return value == null ? null : (Integer)Buff.GCR.apply(value);
	}


    @Override
    public  boolean put(final Buff key,final Buff value, int idx) {
    	Buff keyb = Buff.CC.Copy(key);
    	Buff valueb = Buff.CC.Copy(value);
    	return skipListMap.put(keyb,valueb) == null ? true : false;
    }
    
    @Override
    public  boolean Fill(final Buff key,final Buff value, int idx) {    	
    	Buff keyb = Buff.CC.Copy(key);
    	Buff valueb = Buff.CC.Copy(value);
    	return skipListMap.put(keyb,valueb) == null ? true : false;
    }
    
    @Override
    public  boolean OverWrite(final Buff key,final Buff value, int idx) {
    	
    	Buff valueOff =skipListMap.compute(key, (old,v)->
    	{	
    		old.buffer.putInt(0,~old.buffer.getInt(0));
    		return old;	
    		});
    	if(valueOff == null)
    		return false;
    	else 	
    		return true;
    }
    
    public  boolean FillParallel(int sizeInMillions, int keysize, int valsize, int range) {    	
    	ArrayList<Thread> threads = new ArrayList<>();
    	int NUM_THREADS = sizeInMillions/1_000_000;;
	    for (int i = 0; i < NUM_THREADS; i++) {
	    	threads.add(new Thread(new FillerThread(i, skipListMap, keysize, valsize, range)));
	    	threads.get(i).start();
	    	}	
	    for (Thread thread : threads) {
	        try {
				thread.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
	    }
		return true;
    }


    @Override
    public  boolean remove(final Buff key, int idx) {
    	Buff val = skipListMap.remove(key);
    	return val == null ? false : true;
    }


    @Override
    public void clear() {

        skipListMap = null;
        skipListMap = new ConcurrentSkipListMap<>();
        System.gc();
    }

    @Override
    public int Size() {
        return skipListMap.size();
    }

    @Override
    public long allocated() {
    	return 0;
    }

    @Override
    public void print() {}
    
	public class FillerThread extends Thread {

		int idx;
		Random localRanom;
		Buff keybuf;
		Buff valbuf;
		int range;
		ConcurrentSkipListMap map;
		
		FillerThread(int index, ConcurrentSkipListMap local, int keysize, int valsize, int range){
			idx = index;
			map = local;
			this.range = range;
			keybuf = new Buff(keysize);
			valbuf = new Buff(valsize);
			localRanom = new Random(idx);
		}
		
		@Override
		public void run() {
			int i = 0;
			int v ;
			while( i < 1_000_000) {		
				v = localRanom.nextInt(this.range);
				keybuf.set(v);
				valbuf.set(v);
		    	Buff keyb = Buff.CC.Copy(keybuf);
		    	Buff valb = Buff.CC.Copy(valbuf);
				if(map.put(keyb,valb) != null)
					i--;
				i++;
				}
			}
		}
	
    static public void main(String[]arg) {
    	SkipList_OnHeap myskip = new SkipList_OnHeap();
    	Buff x = new Buff(4);
    			x.set(0);
    	myskip.put(x, x, 0);
    	myskip.OverWrite(x, x, 0);
    	Buff y = new Buff(4);
    	y.set(1);
    	myskip.OverWrite(y, y, 0);
    	int t = myskip.containsKey(x, 0);
    }

}
