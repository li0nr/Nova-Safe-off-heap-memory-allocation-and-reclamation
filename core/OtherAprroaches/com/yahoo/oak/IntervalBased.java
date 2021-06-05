package com.yahoo.oak;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicLong;

import sun.misc.Unsafe;

//public class IntervalBased {
//	
//	
//    private final int             maxThreads= 32;
//    private int[] releasecounter = new int[maxThreads*CLPAD];
//	private static final int CLPAD = 33;
//
//    private final AtomicLong eraClock;
//    private long[] upper_reserve = new long[maxThreads*CLPAD];
//    private long[] lower_reserve = new long[maxThreads*CLPAD];
//    private long[] retire_counters = new long[maxThreads*CLPAD];
//    private long[] alloc_counters = new long[maxThreads*CLPAD];
//    
//    private final ArrayList<HazardEras_interface>[] retiredList= new ArrayList[maxThreads*CLPAD];//CLPAD is for cache padding
//    private final NativeMemoryAllocator allocator;
//    
//	static final Unsafe UNSAFE=UnsafeUtils.unsafe;
//	
//	
//	
//	class IBSlice extends NovaSlice implements HazardEras_interface{
//		private long bornEra;
//		private long deadEra;
//		
//		IBSlice(long Era){
//			super(0,0,0);
//			bornEra = Era;
//		}
//		
//		 public void setDeleteEra(long Era){
//			 deadEra = Era;
//		 }
//		 
//		 public void setEra(long Era) {
//			 bornEra = Era;
//		 }
//
//		 public long getnewEra() {
//			 return bornEra;
//		 }
//		 
//		 public long getdelEra() {
//			 return deadEra;
//		 }
//	}
//	
//	IntervalBased(){
//		
//	}
//
//	
//}

//
//public:
//	~RangeTrackerNew(){};
//	RangeTrackerNew(int task_num, int epochFreq, int emptyFreq, bool collect): 
//	 BaseTracker<T>(task_num),task_num(task_num),freq(emptyFreq),epochFreq(epochFreq),collect(collect){
//		retired = new padded<std::list<RangeTrackerNew<T>::IntervalInfo>>[task_num];
//		upper_reservs = new paddedAtomic<uint64_t>[task_num];
//		lower_reservs = new paddedAtomic<uint64_t>[task_num];
//		for (int i = 0; i < task_num; i++){
//			upper_reservs[i].ui.store(UINT64_MAX, std::memory_order_release);
//			lower_reservs[i].ui.store(UINT64_MAX, std::memory_order_release);
//		}
//		retire_counters = new padded<uint64_t>[task_num];
//		alloc_counters = new padded<uint64_t>[task_num];
//		epoch.store(0,std::memory_order_release);
//	}
//	RangeTrackerNew(int task_num, int epochFreq, int emptyFreq) : RangeTrackerNew(task_num,epochFreq,emptyFreq,true){}
//
//	void __attribute__ ((deprecated)) reserve(uint64_t e, int tid){
//		return reserve(tid);
//	}
//	uint64_t get_epoch(){
//		return epoch.load(std::memory_order_acquire);
//	}
//
//	void* alloc(int tid){
//		alloc_counters[tid] = alloc_counters[tid]+1;
//		if(alloc_counters[tid]%(epochFreq*task_num)==0){
//			epoch.fetch_add(1,std::memory_order_acq_rel);
//		}
//		char* block = (char*) malloc(sizeof(uint64_t) + sizeof(T));
//		uint64_t* birth_epoch = (uint64_t*)(block + sizeof(T));
//		*birth_epoch = get_epoch();
//		return (void*)block;
//	}
//
//	static uint64_t read_birth(T* obj){
//		uint64_t* birth_epoch = (uint64_t*)((char*)obj + sizeof(T));
//		return *birth_epoch;
//	}
//
//	void reclaim(T* obj){
//		obj->~T();
//		free ((char*)obj);
//	}
//
//	T* read(std::atomic<T*>& obj, int idx, int tid){
//		return read(obj, tid);
//	}
//    T* read(std::atomic<T*>& obj, int tid){
//        uint64_t prev_epoch = upper_reservs[tid].ui.load(std::memory_order_acquire);
//		while(true){
//			T* ptr = obj.load(std::memory_order_acquire);
//			uint64_t curr_epoch = get_epoch();
//			if (curr_epoch == prev_epoch){
//				return ptr;
//			} else {
//				// upper_reservs[tid].ui.store(curr_epoch, std::memory_order_release);
//				upper_reservs[tid].ui.store(curr_epoch, std::memory_order_seq_cst);
//				prev_epoch = curr_epoch;
//			}
//		}
//    }
//
//	void start_op(int tid){
//		uint64_t e = epoch.load(std::memory_order_acquire);
//		lower_reservs[tid].ui.store(e,std::memory_order_seq_cst);
//		upper_reservs[tid].ui.store(e,std::memory_order_seq_cst);
//		// lower_reservs[tid].ui.store(e,std::memory_order_release);
//		// upper_reservs[tid].ui.store(e,std::memory_order_release);
//	}
//	void end_op(int tid){
//		upper_reservs[tid].ui.store(UINT64_MAX,std::memory_order_release);
//		lower_reservs[tid].ui.store(UINT64_MAX,std::memory_order_release);
//	}
//	void reserve(int tid){
//		start_op(tid);
//	}
//	void clear(int tid){
//		end_op(tid);
//	}
//
//	
//	inline void incrementEpoch(){
//		epoch.fetch_add(1,std::memory_order_acq_rel);
//	}
//	
//	void retire(T* obj, uint64_t birth_epoch, int tid){
//		if(obj==NULL){return;}
//		std::list<IntervalInfo>* myTrash = &(retired[tid].ui);
//		// for(auto it = myTrash->begin(); it!=myTrash->end(); it++){
//		// 	assert(it->obj!=obj && "double retire error");
//		// }
//			
//		uint64_t retire_epoch = epoch.load(std::memory_order_acquire);
//		myTrash->push_back(IntervalInfo(obj, birth_epoch, retire_epoch));
//		retire_counters[tid]=retire_counters[tid]+1;
//		if(collect && retire_counters[tid]%freq==0){
//			empty(tid);
//		}
//	}
//
//	void retire(T* obj, int tid){
//		retire(obj, read_birth(obj), tid);
//	}
//	
//	bool conflict(uint64_t* lower_epochs, uint64_t* upper_epochs, uint64_t birth_epoch, uint64_t retire_epoch){
//		for (int i = 0; i < task_num; i++){
//			if (upper_epochs[i] >= birth_epoch && lower_epochs[i] <= retire_epoch){
//				return true;
//			}
//		}
//		return false;
//	}
//
//	void empty(int tid){
//		//read all epochs
//		uint64_t upper_epochs_arr[task_num];
//		uint64_t lower_epochs_arr[task_num];
//		for (int i = 0; i < task_num; i++){
//			//sequence matters.
//			lower_epochs_arr[i] = lower_reservs[i].ui.load(std::memory_order_acquire);
//			upper_epochs_arr[i] = upper_reservs[i].ui.load(std::memory_order_acquire);
//		}
//
//		// erase safe objects
//		std::list<IntervalInfo>* myTrash = &(retired[tid].ui);
//		for (auto iterator = myTrash->begin(), end = myTrash->end(); iterator != end; ) {
//			IntervalInfo res = *iterator;
//			if(!conflict(lower_epochs_arr, upper_epochs_arr, res.birth_epoch, res.retire_epoch)){
//				reclaim(res.obj);
//				this->dec_retired(tid);
//				iterator = myTrash->erase(iterator);
//			}
//			else{++iterator;}
//		}
//	}
//
//	bool collecting(){return collect;}
//};
