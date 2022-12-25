package com.nova;

import org.junit.Test;


	public class ListHETest {
		static final int  LIST_SIZE = 50;
		static final int  MAGIC_NUM = 96;

		List_HE list = new List_HE();
		
		@Test
		public void NovaListFill(){
			for(int i=0; i< LIST_SIZE; i++) {
				list.add((long)MAGIC_NUM-i, 0);
			}
			for(int i=0; i< LIST_SIZE; i++) {
				assert list.get(i, 0) == MAGIC_NUM-i : "NOT right";
			}
		}
		
		@Test
		public void NovaListFillWrite(){
			for(int i=0; i< LIST_SIZE; i++) {
				list.add((long)MAGIC_NUM-i, 0);
			}
			for(int i=0; i< LIST_SIZE; i++) {
				list.set(i,((long)MAGIC_NUM-i)*2, 0);
			}
			for(int i=0; i< LIST_SIZE; i++) {
				assert list.get(i, 0) == 2*(MAGIC_NUM-i) : "NOT right";
			}
		}
}
