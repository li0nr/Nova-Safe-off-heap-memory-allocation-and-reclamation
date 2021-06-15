package com.yahoo.oak;

/**
*  This is an implementation of the non-blocking, concurrent binary search tree of
*  Faith Ellen, Panagiota Fatourou, Eric Ruppert and Franck van Breugel.
*
*  Copyright (C) 2011  Trevor Brown, Joanna Helga
*  Contact Trevor Brown (tabrown@cs.toronto.edu) with any questions or comments.
*
*  This program is free software: you can redistribute it and/or modify
*  it under the terms of the GNU General Public License as published by
*  the Free Software Foundation, either version 3 of the License, or
*  (at your option) any later version.
*
*  This program is distributed in the hope that it will be useful,
*  but WITHOUT ANY WARRANTY; without even the implied warranty of
*  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*  GNU General Public License for more details.
*
*  You should have received a copy of the GNU General Public License
*  along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import com.yahoo.oak.HazardEras.HEslice;

public class BST_NoMM<K , V> {
	
	   final NovaSerializer<K> SrzK;
	   final NovaSerializer<V> SrzV;
	   final NovaC<K> KCt;
	   final NovaC<V> VCt;
	   final NativeMemoryAllocator alloc;
	   
	   public int del_count = 0;
	   public int put_count = 0;
	   public int get_count = 0;

   //--------------------------------------------------------------------------------
   // Class: Node
   //--------------------------------------------------------------------------------
   protected final static class Node<E , V> {
       final NovaSlice key;
       final NovaSlice value;
       volatile Node<NovaSlice, NovaSlice> left;
       volatile Node<NovaSlice, NovaSlice> right;
       volatile Info<NovaSlice, NovaSlice> info;

       /** FOR MANUAL CREATION OF NODES (only used directly by testbed) **/
       Node(final NovaSlice key, final NovaSlice value, final Node<NovaSlice, NovaSlice> left, final Node<NovaSlice, NovaSlice> right) {
           this.key = key;
           this.value = value;
           this.left = left;
           this.right = right;
           this.info = null;
       }

       /** TO CREATE A LEAF NODE **/
       Node(final NovaSlice key, final NovaSlice value) {
           this(key, value, null, null);
       }

       /** TO CREATE AN INTERNAL NODE **/
       Node(final NovaSlice key, final Node<NovaSlice, NovaSlice> left, final Node<NovaSlice, NovaSlice> right) {
           this(key, null, left, right);
       }
   }

   //--------------------------------------------------------------------------------
   // Class: Info, DInfo, IInfo, Mark, Clean
   // May 25th: trying to make CAS to update field static
   // instead of using <state, Info>, we extends Info to all 4 states
   // to see a state of a node, see what kind of Info class it has
   //--------------------------------------------------------------------------------
   protected static abstract class Info<E extends Comparable<? super E>, V> {
   }

   protected final static class DInfo<E extends Comparable<? super E>, V> extends Info<E,V> {
       final Node<NovaSlice, NovaSlice> p;
       final Node<NovaSlice, NovaSlice> l;
       final Node<NovaSlice, NovaSlice> gp;
       final Info<NovaSlice, NovaSlice> pinfo;

       DInfo(final Node<NovaSlice, NovaSlice> leaf, final Node<NovaSlice, NovaSlice> parent, final Node<NovaSlice, NovaSlice> grandparent, final Info<NovaSlice, NovaSlice> pinfo) {
           this.p = parent;
           this.l = leaf;
           this.gp = grandparent;
           this.pinfo = pinfo;
       }
   }

   protected final static class IInfo<E extends Comparable<? super E>, V> extends Info<E,V> {
       final Node<NovaSlice, NovaSlice> p;
       final Node<NovaSlice, NovaSlice> l;
       final Node<NovaSlice, NovaSlice> newInternal;

       IInfo(final Node<NovaSlice, NovaSlice> leaf, final Node <NovaSlice, NovaSlice> parent, final Node<NovaSlice, NovaSlice> newInternal){
           this.p = parent;
           this.l = leaf;
           this.newInternal = newInternal;
       }
   }

   protected final static class Mark<E extends Comparable<? super E>, V> extends Info<E,V> {
       final DInfo<NovaSlice, NovaSlice> dinfo;

       Mark(final DInfo<NovaSlice, NovaSlice> dinfo) {
           this.dinfo = dinfo;
       }
   }

   protected final static class Clean<E extends Comparable<? super E>, V> extends Info<E,V> {}
//--------------------------------------------------------------------------------
//DICTIONARY
//--------------------------------------------------------------------------------
   private static final AtomicReferenceFieldUpdater<Node, Node> leftUpdater = AtomicReferenceFieldUpdater.newUpdater(Node.class, Node.class, "left");
   private static final AtomicReferenceFieldUpdater<Node, Node> rightUpdater = AtomicReferenceFieldUpdater.newUpdater(Node.class, Node.class, "right");
   private static final AtomicReferenceFieldUpdater<Node, Info> infoUpdater = AtomicReferenceFieldUpdater.newUpdater(Node.class, Info.class, "info");

   final Node<NovaSlice, NovaSlice> root;

   public BST_NoMM(NovaSerializer<K> sK, NovaSerializer<V> sV,
		   		 NovaC<K> cmpK , NovaC<V> cmpV, NativeMemoryAllocator alloc) {
       // to avoid handling special case when <= 2 nodes,
       // create 2 dummy nodes, both contain key null
       // All real keys inside BST are required to be non-null
	   SrzK = sK; SrzV = sV;
	   KCt = cmpK; VCt = cmpV;
	   this.alloc = alloc;
       root = new Node<NovaSlice, NovaSlice>(null, new Node<NovaSlice, NovaSlice>(null, null), new Node<NovaSlice, NovaSlice>(null, null));
   }

//--------------------------------------------------------------------------------
//PUBLIC METHODS:
//- find   : boolean
//- insert : boolean
//- delete : boolean
//--------------------------------------------------------------------------------

   /** PRECONDITION: k CANNOT BE NULL **/
   public final boolean containsKey(final K key, int idx) {
	   get_count ++;
	   
       if (key == null) throw new NullPointerException();
       Node<NovaSlice, NovaSlice> l = root.left;
       while (l.left != null) {
           l = (l.key == null || KCt.compareKeys(l.key.address + l.key.offset, key) > 0) ? l.left : l.right;
       }
       boolean ret = (l.key != null && KCt.compareKeys(l.key.address + l.key.offset, key) == 0) ? true : false;
       return ret;
   }

   /** PRECONDITION: k CANNOT BE NULL **/
   public final V get(final K key, int idx) {
	   get_count ++;

       if (key == null) throw new NullPointerException();
       NovaSlice access;
       int i = 0;
       Node<NovaSlice, NovaSlice> l = root.left;
       while (l.left != null) {
           l = (l.key == null || KCt.compareKeys(l.key.address + l.key.offset, key) > 0) ? l.left : l.right;
           i ++;
       }
       V ret = (l.key != null && KCt.compareKeys(l.key.address + l.key.offset, key)  == 0) ? 
    		   SrzV.deserialize(l.value.address + l.value.offset) : null;
       return ret;
   }

   // Insert key to dictionary, return the previous value associated with the specified key,
   // or null if there was no mapping for the key
   /** PRECONDITION: k CANNOT BE NULL **/
   public final V put(final K key, final V value, int idx) {
	   put_count ++;

       Node<NovaSlice, NovaSlice> newInternal;
       Node<NovaSlice, NovaSlice> newSibling, newNode;
       IInfo<NovaSlice, NovaSlice> newPInfo;
       NovaSlice result;

       /** SEARCH VARIABLES **/
       Node<NovaSlice, NovaSlice> p;
       Info<NovaSlice, NovaSlice> pinfo;
       Node<NovaSlice, NovaSlice> l;
       /** END SEARCH VARIABLES **/
       NovaSlice k = new NovaSlice(0, 0, 0);
       NovaSlice v = new NovaSlice(0, 0, 0);
       alloc.allocate(k, SrzK.calculateSize(key));
       alloc.allocate(v, SrzV.calculateSize(value));

       newNode = new Node<NovaSlice, NovaSlice>(k,v);
       SrzK.serialize(key,   newNode.key.address   +newNode.key.offset);
       SrzV.serialize(value, newNode.value.address + newNode.value.offset);
       
       while (true) {

           /** SEARCH **/
           p = root;
           pinfo = p.info;
           l = p.left;
           while (l.left != null) {
               p = l;
               l = (l.key == null || KCt.compareKeys(l.key.address+l.key.offset, key) > 0) ? l.left : l.right;
           }
           pinfo = p.info;                             // read pinfo once instead of every iteration
           if (l != p.left && l != p.right) continue;  // then confirm the child link to l is valid
                                                       // (just as if we'd read p's info field before the reference to l)
           /** END SEARCH **/

           if (!(pinfo == null || pinfo.getClass() == Clean.class)) {
               help(pinfo);
           } else {
               if (l.key != null && KCt.compareKeys(l.key.address+l.key.offset, key)  == 0) {
                   // key already in the tree, try to replace the old node with new node
                   result = l.value;
                   V ret = SrzV.deserialize(result.address+ result.offset);
                   alloc.free(newNode.key);
                   alloc.free(newNode.value);
                   return ret;
               } else {
                   // key is not in the tree, try to replace a leaf with a small subtree
                   newSibling = new Node<NovaSlice, NovaSlice>(l.key, l.value);
                   if (l.key == null ||  KCt.compareKeys(l.key.address+l.key.offset, key) > 0) // newinternal = max(ret.l.key, key);
                	   {
                       if(l.key != null) {
                    	   NovaSlice tmp = new NovaSlice(0, 0, 0);
                    	   alloc.allocate(tmp, SrzK.calculateSize(key));
                    	   SrzK.serialize(l.key.address+l.key.offset, tmp.address+tmp.offset);
                       }                           
                   	   newInternal = new Node<NovaSlice, NovaSlice>(l.key, newNode, newSibling);
                   	   
                	   } else {
                	       NovaSlice tmp = new NovaSlice(0, 0, 0);
                	       alloc.allocate(tmp, SrzK.calculateSize(key));
                	       SrzK.serialize(key,   tmp.address   +tmp.offset);
                		   newInternal = new Node<NovaSlice, NovaSlice>(tmp, newSibling, newNode);
                		   }

                   newPInfo = new IInfo<NovaSlice, NovaSlice>(l, p, newInternal);
                   result = null;
                   }
               
               // try to IFlag parent
               if (infoUpdater.compareAndSet(p, pinfo, newPInfo)) {
                   helpInsert(newPInfo);
                   return null;
               } else {
                   // if fails, help the current operation
                   // need to get the latest p.info since CAS doesnt return current value
                   alloc.free(newInternal.key);
                   help(p.info);
                   }
               }
           }
       }

   // Delete key from dictionary, return the associated value when successful, null otherwise
   /** PRECONDITION: k CANNOT BE NULL **/
   public final boolean remove(final K key, int idx){
	   del_count++;

       /** SEARCH VARIABLES **/
       Node<NovaSlice, NovaSlice> gp;
       Info<NovaSlice, NovaSlice> gpinfo;
       Node<NovaSlice, NovaSlice> p;
       Info<NovaSlice, NovaSlice> pinfo;
       Node<NovaSlice, NovaSlice> l;
       /** END SEARCH VARIABLES **/
       
       while (true) {

           /** SEARCH **/
           gp = null;
           gpinfo = null;
           p = root;
           pinfo = p.info;
           l = p.left;
           while (l.left != null) {
               gp = p;
               p = l;
               l = (l.key == null || KCt.compareKeys(l.key.address+l.key.offset, key) > 0) ? l.left : l.right;
           }
           // note: gp can be null here, because clearly the root.left.left == null
           //       when the tree is empty. however, in this case, l.key will be null,
           //       and the function will return null, so this does not pose a problem.
           if (gp != null) {
               gpinfo = gp.info;                               // - read gpinfo once instead of every iteration
               if (p != gp.left && p != gp.right) continue;    //   then confirm the child link to p is valid
               pinfo = p.info;                                 //   (just as if we'd read gp's info field before the reference to p)
               if (l != p.left && l != p.right) continue;      // - do the same for pinfo and l
           }
           /** END SEARCH **/

           if (l.key == null || KCt.compareKeys(l.key.address+l.key.offset, key) != 0) {
        	   return false;
           }
           if (!(gpinfo == null || gpinfo.getClass() == Clean.class)) {
               help(gpinfo);
           } else if (!(pinfo == null || pinfo.getClass() == Clean.class)) {
               help(pinfo);
           } else {
               // try to DFlag grandparent
               final DInfo<NovaSlice, NovaSlice> newGPInfo = new DInfo<NovaSlice, NovaSlice>(l, p, gp, pinfo);

               if (infoUpdater.compareAndSet(gp, gpinfo, newGPInfo)) {
                   if (helpDelete(newGPInfo)) {
                	   return true;
                   }
               } else {
                   // if fails, help grandparent with its latest info value
                   help(gp.info);
               }
           }
       }
   }

//--------------------------------------------------------------------------------
//PRIVATE METHODS
//- helpInsert
//- helpDelete
//--------------------------------------------------------------------------------

   private void helpInsert(final IInfo<NovaSlice, NovaSlice> info){
       (info.p.left == info.l ? leftUpdater : rightUpdater).compareAndSet(info.p, info.l, info.newInternal);
       infoUpdater.compareAndSet(info.p, info, new Clean());
   }

   private boolean helpDelete(final DInfo<NovaSlice, NovaSlice> info){
       final boolean result;

       result = infoUpdater.compareAndSet(info.p, info.pinfo, new Mark<NovaSlice, NovaSlice>(info));
       final Info<NovaSlice, NovaSlice> currentPInfo = info.p.info;
       // if  CAS succeed or somebody else already suceed helping, the helpMarked
       if (result || (currentPInfo.getClass() == Mark.class && ((Mark<NovaSlice, NovaSlice>) currentPInfo).dinfo == info)) {
           helpMarked(info);
           return true;
       } else {
           help(currentPInfo);
           infoUpdater.compareAndSet(info.gp, info, new Clean());
           return false;
       }
   }

   private void help(final Info<NovaSlice, NovaSlice> info) {
       if (info.getClass() == IInfo.class)     helpInsert((IInfo<NovaSlice, NovaSlice>) info);
       else if(info.getClass() == DInfo.class) helpDelete((DInfo<NovaSlice, NovaSlice>) info);
       else if(info.getClass() == Mark.class)  helpMarked(((Mark<NovaSlice, NovaSlice>)info).dinfo);
   }

   private void helpMarked(final DInfo<NovaSlice, NovaSlice> info) {
       final Node<NovaSlice, NovaSlice> other = (info.p.right == info.l) ? info.p.left : info.p.right;
       (info.gp.left == info.p ? leftUpdater : rightUpdater).compareAndSet(info.gp, info.p, other);
       alloc.free(info.l.key);
       alloc.free(info.l.value);
       if(info.p.key != null ) alloc.free(info.p.key);
       infoUpdater.compareAndSet(info.gp, info, new Clean());
   }

   /**
    *
    * DEBUG CODE (FOR TESTBED)
    *
    */

   private int sumDepths(Node node, int depth) {
       if (node == null) return 0;
       if (node.left == null && node.key != null) return depth;
       return sumDepths(node.left, depth+1) + sumDepths(node.right, depth+1);
   }

   public final int getSumOfDepths() {
       return sumDepths(root, 0);
   }

   /**
    * size() is NOT a constant time method, and the result is only guaranteed to
    * be consistent if no concurrent updates occur.
    * Note: linearizable size() and iterators can be implemented, so contact
    *       the author if they are needed for some application.
    */
   public final int size() {
       return sequentialSize(root);
   }
   private int sequentialSize(final Node node) {
       if (node == null) return 0;
       if (node.left == null && node.key != null) return 1;
       return sequentialSize(node.left) + sequentialSize(node.right);
   }

}
