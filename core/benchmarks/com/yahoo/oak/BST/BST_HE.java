package com.yahoo.oak.BST;

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

import com.yahoo.oak.HazardEras;
import com.yahoo.oak.NativeMemoryAllocator;
import com.yahoo.oak.NovaC;
import com.yahoo.oak.NovaS;
import com.yahoo.oak.HazardEras.HEslice;

public class BST_HE<K , V> {
	
	   final NovaS<K> SrzK;
	   final NovaS<V> SrzV;
	   final NovaC<K> KCt;
	   final NovaC<V> VCt;
	   final HazardEras HE;
	   
	   public int del_count = 0;
	   public int put_count = 0;
	   public int get_count = 0;

   //--------------------------------------------------------------------------------
   // Class: Node
   //--------------------------------------------------------------------------------
   protected final static class Node<E , V> {
       final HEslice key;
       final HEslice value;
       volatile Node<HEslice, HEslice> left;
       volatile Node<HEslice, HEslice> right;
       volatile Info<HEslice, HEslice> info;

       /** FOR MANUAL CREATION OF NODES (only used directly by testbed) **/
       Node(final HEslice key, final HEslice value, final Node<HEslice, HEslice> left, final Node<HEslice, HEslice> right) {
           this.key = key;
           this.value = value;
           this.left = left;
           this.right = right;
           this.info = null;
       }

       /** TO CREATE A LEAF NODE **/
       Node(final HEslice key, final HEslice value) {
           this(key, value, null, null);
       }

       /** TO CREATE AN INTERNAL NODE **/
       Node(final HEslice key, final Node<HEslice, HEslice> left, final Node<HEslice, HEslice> right) {
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
       final Node<HEslice, HEslice> p;
       final Node<HEslice, HEslice> l;
       final Node<HEslice, HEslice> gp;
       final Info<HEslice, HEslice> pinfo;

       DInfo(final Node<HEslice, HEslice> leaf, final Node<HEslice, HEslice> parent, final Node<HEslice, HEslice> grandparent, final Info<HEslice, HEslice> pinfo) {
           this.p = parent;
           this.l = leaf;
           this.gp = grandparent;
           this.pinfo = pinfo;
       }
   }

   protected final static class IInfo<E extends Comparable<? super E>, V> extends Info<E,V> {
       final Node<HEslice, HEslice> p;
       final Node<HEslice, HEslice> l;
       final Node<HEslice, HEslice> newInternal;

       IInfo(final Node<HEslice, HEslice> leaf, final Node <HEslice, HEslice> parent, final Node<HEslice, HEslice> newInternal){
           this.p = parent;
           this.l = leaf;
           this.newInternal = newInternal;
       }
   }

   protected final static class Mark<E extends Comparable<? super E>, V> extends Info<E,V> {
       final DInfo<HEslice, HEslice> dinfo;

       Mark(final DInfo<HEslice, HEslice> dinfo) {
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

    final Node<HEslice, HEslice> root;

    public BST_HE(NovaS<K> sK, NovaS<V> sV,
 		   		 NovaC<K> cmpK , NovaC<V> cmpV, NativeMemoryAllocator alloc) {
        // to avoid handling special case when <= 2 nodes,
        // create 2 dummy nodes, both contain key null
        // All real keys inside BST are required to be non-null
 	   SrzK = sK; SrzV = sV;
 	   KCt = cmpK; VCt = cmpV;
 	   HE = new HazardEras(alloc);
        root = new Node<HEslice, HEslice>(null, new Node<HEslice, HEslice>(null, null), new Node<HEslice, HEslice>(null, null));
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
        HEslice access;
    	Retry: while (true){
    		try {	
    	        Node<HEslice, HEslice> l = root.left;
    			while (l.left != null) {
    				access = HE.get_protected(l.key,idx);
    				l = (access == null || KCt.compareKeys(access.address + access.offset, key) > 0) ? l.left : l.right;
    			}
    			access = HE.get_protected(l.key,idx);
    			boolean ret = (l.key != null && KCt.compareKeys(access.address + access.offset, key) == 0) ? true : false;
    			HE.clear(idx);
    			return ret;
    			}catch (Exception e) { continue Retry; }
    		}
    }

    /** PRECONDITION: k CANNOT BE NULL **/
    public final V get(final K key, int idx) {
    	get_count ++;

        if (key == null) throw new NullPointerException();
        HEslice access;
    	Retry: while (true){
            Node<HEslice, HEslice> l = root.left;
    		try {	
	        while (l.left != null) {
	     	   access = HE.get_protected(l.key,idx);
	            l = (access == null || (int)KCt.compareKeys(access.address + access.offset, key) > 0) ? l.left : l.right;
	        }
	        access = HE.get_protected(l.key,idx);
	        V ret = (l.key != null && KCt.compareKeys(access.address+ access.offset, key)  == 0) ? 
	     		   SrzV.deserialize(l.value.address + l.value.offset) : null;
	        HE.clear(idx);
	        return ret;
			}catch (Exception e) {
	    		continue Retry; //Facade throws
	    		}
	    	}
	    }

    // Insert key to dictionary, return the previous value associated with the specified key,
    // or null if there was no mapping for the key
    /** PRECONDITION: k CANNOT BE NULL **/
    public final V put(final K key, final V value, int idx) {
    	put_count ++;
    	
        Node<HEslice, HEslice> newInternal;
        Node<HEslice, HEslice> newSibling, newNode;
        IInfo<HEslice, HEslice> newPInfo;
        HEslice result;

        /** SEARCH VARIABLES **/
        Node<HEslice, HEslice> p;
        Info<HEslice, HEslice> pinfo;
        Node<HEslice, HEslice> l;
        /** END SEARCH VARIABLES **/
        newNode = new Node<HEslice, HEslice>(HE.allocate(SrzK.calculateSize(key)),
     		   							   HE.allocate(SrzV.calculateSize(value)));
        SrzK.serialize(key,   newNode.key.address   +newNode.key.offset);
        SrzV.serialize(value, newNode.value.address + newNode.value.offset);

        HEslice access;
       Redo:
        while (true) {
        	try {
            /** SEARCH **/
            p = root;
            pinfo = p.info;
            l = p.left;
            while (l.left != null) {
                p = l;
                access = HE.get_protected(l.key,idx);
                l = (access == null || KCt.compareKeys(access.address+access.offset, key) > 0) ? l.left : l.right;
            }
            pinfo = p.info;                             // read pinfo once instead of every iteration
            if (l != p.left && l != p.right) continue;  // then confirm the child link to l is valid
                                                        // (just as if we'd read p's info field before the reference to l)
            /** END SEARCH **/

            if (!(pinfo == null || pinfo.getClass() == Clean.class)) {
                help(pinfo, idx);
            } else {
         	   access = HE.get_protected(l.key,idx);
                if (access != null && KCt.compareKeys(access.address+access.offset, key)  == 0) {
                    // key already in the tree, try to replace the old node with new node
                	result = HE.get_protected(l.value,idx);
                	V objret = SrzV.deserialize(result.address+ result.offset);
                	HE.fastFree(newNode.key);
                	HE.fastFree(newNode.value);
                	return objret;
                	
                } else {
                    // key is not in the tree, try to replace a leaf with a small subtree
                    newSibling = new Node<HEslice, HEslice>(l.key, l.value);
                    access = HE.get_protected(l.key,idx);
                    if (access == null ||  KCt.compareKeys(access.address+access.offset, key) > 0) // newinternal = max(ret.l.key, key);
                 	   {
                    	HEslice tmp = null;
                        if(access != null) {
                            tmp = HE.allocate(SrzK.calculateSize(key));
                        	SrzK.serialize(access.address+access.offset,   tmp.address   +tmp.offset);

                        }
                        newInternal = new Node<HEslice, HEslice>(tmp, newNode, newSibling);
                 	   } else {
                           HEslice tmp = HE.allocate(SrzK.calculateSize(key));
                           SrzK.serialize(key,   tmp.address   +tmp.offset);
                 		   newInternal = new Node<HEslice, HEslice>(tmp, newSibling, newNode);
                 		   }
                    newPInfo = new IInfo<HEslice, HEslice>(l, p, newInternal);
                    result = null;
                    }
                
                // try to IFlag parent
                if (infoUpdater.compareAndSet(p, pinfo, newPInfo)) {
                    helpInsert(newPInfo);
                    return null;
                } else {
                    // if fails, help the current operation
                    // need to get the latest p.info since CAS doesnt return current value
                    HE.fastFree(newInternal.key);
                	help(p.info, idx);
                    return null;
                    }
                }
            }catch (Exception e) { continue Redo;}
        }
    }

    // Delete key from dictionary, return the associated value when successful, null otherwise
    /** PRECONDITION: k CANNOT BE NULL **/
    public final boolean remove(final K key, int idx){
    	del_count++;
    	
        /** SEARCH VARIABLES **/
        Node<HEslice, HEslice> gp;
        Info<HEslice, HEslice> gpinfo;
        Node<HEslice, HEslice> p;
        Info<HEslice, HEslice> pinfo;
        Node<HEslice, HEslice> l;
        /** END SEARCH VARIABLES **/
        HEslice access;
        Redo:
        while (true) {
        	try {
            /** SEARCH **/
            gp = null;
            gpinfo = null;
            p = root;
            pinfo = p.info;
            l = p.left;
            while (l.left != null) {
                gp = p;
                p = l;
                access = HE.get_protected(l.key,idx);
                l = (access == null || KCt.compareKeys(access.address+access.offset, key) > 0) ? l.left : l.right;
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
     	   access = HE.get_protected(l.key,idx);

            if (access == null || KCt.compareKeys(access.address+access.offset, key) != 0) {
         	   HE.clear(idx);
         	   return false;
            }
            if (!(gpinfo == null || gpinfo.getClass() == Clean.class)) {
                help(gpinfo, idx);
            } else if (!(pinfo == null || pinfo.getClass() == Clean.class)) {
                help(pinfo, idx);
            } else {
                // try to DFlag grandparent
                final DInfo<HEslice, HEslice> newGPInfo = new DInfo<HEslice, HEslice>(l, p, gp, pinfo);

                if (infoUpdater.compareAndSet(gp, gpinfo, newGPInfo)) {
                    if (helpDelete(newGPInfo, idx)) {
                 	   return true;
                    }
                } else {
                    // if fails, help grandparent with its latest info value
                    help(gp.info, idx);
                    }
                }
            }catch (Exception e) { continue Redo;}
        	}
    }
 //--------------------------------------------------------------------------------
 //PRIVATE METHODS
 //- helpInsert
 //- helpDelete
 //--------------------------------------------------------------------------------

    private void helpInsert(final IInfo<HEslice, HEslice> info){
        (info.p.left == info.l ? leftUpdater : rightUpdater).compareAndSet(info.p, info.l, info.newInternal);
        infoUpdater.compareAndSet(info.p, info, new Clean());
    }

    private boolean helpDelete(final DInfo<HEslice, HEslice> info, int idx){
        final boolean result;

        result = infoUpdater.compareAndSet(info.p, info.pinfo, new Mark<HEslice, HEslice>(info));
        final Info<HEslice, HEslice> currentPInfo = info.p.info;
        // if  CAS succeed or somebody else already suceed helping, the helpMarked
        if (result || (currentPInfo.getClass() == Mark.class && ((Mark<HEslice, HEslice>) currentPInfo).dinfo == info)) {
            helpMarked(info, idx);
            return true;
        } else {
            help(currentPInfo,idx);
            infoUpdater.compareAndSet(info.gp, info, new Clean());
            return false;
        }
    }

    private void help(final Info<HEslice, HEslice> info, int idx) {
        if (info.getClass() == IInfo.class)     helpInsert((IInfo<HEslice, HEslice>) info);
        else if(info.getClass() == DInfo.class) helpDelete((DInfo<HEslice, HEslice>) info, idx);
        else if(info.getClass() == Mark.class)  helpMarked(((Mark<HEslice, HEslice>)info).dinfo, idx);
    }

    private void helpMarked(final DInfo<HEslice, HEslice> info, int idx) {
        final Node<HEslice, HEslice> other = (info.p.right == info.l) ? info.p.left : info.p.right;
        (info.gp.left == info.p ? leftUpdater : rightUpdater).compareAndSet(info.gp, info.p, other);
        HE.clear(idx);
        HE.retire(idx,info.l.key);
        HE.retire(idx,info.l.value);
        if(info.p.key != null )  HE.retire(idx,info.p.key);
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
   
   public HazardEras getHE() {
	   return HE;
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
   
   public void Print() {
       Node<HEslice,HEslice> l = root.left;
       PrintAux(root.left);
       System.out.print("****\n");
}
public void PrintAux(Node key) {
	try {
		if ( key.left != null) {
			System.out.print("left ");
			PrintAux(key.left);
			}
		KCt.Print(key.key.address+key.key.offset);;
		if (key.right != null) {
			System.out.print("right ");
			PrintAux(key.right);
			}
		}catch (Exception e) {}
	}
}
