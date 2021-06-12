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
import sun.misc.Unsafe;

public class BST_Nova<K , V> {
	   final NovaSerializer<K> SrzK;
	   final NovaSerializer<V> SrzV;
	   final NovaC<K> KCt;
	   final NovaC<V> VCt;
	   
	   public int del_count = 0;
	   public int put_count = 0;
	   public int get_count = 0;
	   
	   static final long Facade_long_offset_key;
	   static final long Facade_long_offset_value;
	   static final long Illegal_facade = 1;
		static {
			try {
				final Unsafe UNSAFE=UnsafeUtils.unsafe;
				Facade_long_offset_key= UNSAFE.objectFieldOffset
					    (Node.class.getDeclaredField("key"));
				Facade_long_offset_value = UNSAFE.objectFieldOffset
						  (Node.class.getDeclaredField("value"));
				 } catch (Exception ex) { throw new Error(ex); }
		}
	   
   //--------------------------------------------------------------------------------
   // Class: Node
   //--------------------------------------------------------------------------------
   protected final static class Node<E , V> {
       final long key;
       final long value;
       volatile Node  left;
       volatile Node  right;
       volatile Info  info;

       /** FOR MANUAL CREATION OF NODES (only used directly by testbed) **/
       Node(final long key, final long value,
    		   	final Node left, final Node  right) {
           this.key = key;
           this.value = value;
           this.left = left;
           this.right = right;
           this.info = null;
       }

       /** TO CREATE A LEAF NODE **/
       Node(final long key, final long value) {
           this(key, value, null, null);
       }

       /** TO CREATE AN INTERNAL NODE **/
       Node(final long key, final Node left, final Node right) {
           this(key, Illegal_facade, left, right);
       }
   }

   //--------------------------------------------------------------------------------
   // Class: Info, DInfo, IInfo, Mark, Clean
   // May 25th: trying to make CAS to update field static
   // instead of using <state, Info>, we extends Info to all 4 states
   // to see a state of a node, see what kind of Info class it has
   //--------------------------------------------------------------------------------
   protected static abstract class Info<E , V> {
   }

   protected final static class DInfo<E , V> extends Info<E,V> {
       final Node p;
       final Node l;
       final Node gp;
       final Info pinfo;

       DInfo(final Node leaf, final Node parent,
    		   final Node grandparent, final Info pinfo) {
           this.p = parent;
           this.l = leaf;
           this.gp = grandparent;
           this.pinfo = pinfo;
       }
   }

   protected final static class IInfo<E, V> extends Info<E,V> {
       final Node p;
       final Node l;
       final Node newInternal;

       IInfo(final Node leaf, final Node parent,
    		   final Node newInternal){
           this.p = parent;
           this.l = leaf;
           this.newInternal = newInternal;
       }
   }

   protected final static class Mark<E , V> extends Info<E,V> {
       final DInfo dinfo;

       Mark(final DInfo dinfo) {
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

    final Node root;

    public BST_Nova(  NovaSerializer<K> sK, NovaSerializer<V> sV,
 		   			NovaC<K> cKt, NovaC<V> cVt, NovaManager mng) {
        // to avoid handling special case when <= 2 nodes,
        // create 2 dummy nodes, both contain key null
        // All real keys inside BST are required to be non-null
    	Facade_Nova<K, Node> dummyK  = new Facade_Nova(mng);
    	Facade_Nova<V, Node> dummyV  = new Facade_Nova(mng); //setting up nova manager

 	   SrzK = sK; SrzV = sV;
 	   KCt = cKt; VCt = cVt;
        root = new Node(Illegal_facade,
     		   new Node(Illegal_facade, Illegal_facade), 
     		   new Node(Illegal_facade, Illegal_facade));
        //       root = new Node<K,V>(null, new Node<K,V>(null, null), new Node<K,V>(null, null));

    }

 //--------------------------------------------------------------------------------
 //PUBLIC METHODS:
 //- find   : boolean
 //- insert : boolean
 //- delete : boolean
 //--------------------------------------------------------------------------------

    /** PRECONDITION: k CANNOT BE NULL **/
    public final boolean containsKey(final K key, int tidx) {
    	get_count ++;
    	try {
    		if (key == null) throw new NullPointerException();
    		Node l = root.left;
    		while (l.left != null) {
    			l = (l.key == Illegal_facade || Facade_Nova.Compare(key, KCt, l.key) > 0) ? l.left : l.right;
    			}
    		return (l.key != Illegal_facade&&  Facade_Nova.Compare(key, KCt, l.key) == 0) ? true : false;   
    		}catch (Exception e) {
    			return false; //Facade throws
    			}
    	}

    /** PRECONDITION: k CANNOT BE NULL **/
    public final V get(final K key, int tidx) {
    	get_count ++;

 	   try {
 	       if (key == null) throw new NullPointerException();
 	       Node l = root.left;
 	       while (l.left != null) {
 	           l = (l.key == Illegal_facade || Facade_Nova.Compare(key, KCt, l.key) > 0) ? l.left : l.right;
 	       }
 	       V ret = (l.key != Illegal_facade&& Facade_Nova.Compare(key, KCt, l.key) == 0) ? 
 	    		   (V)Facade_Nova.Read(SrzV, l.value): null;
 	       return ret;
 	   }catch (Exception e) {
 		   return null; //Facade throws	   
 		   }
 	   }

    // Insert key to dictionary, return the previous value associated with the specified key,
    // or null if there was no mapping for the key
    /** PRECONDITION: k CANNOT BE NULL **/
    public final V put(final K key, final V value, int idx) {
    	put_count++;
    	
        Node newInternal;
        Node newSibling, newNode;
        IInfo newPInfo;
        long result;

        /** SEARCH VARIABLES **/
        Node p;
        Info pinfo;
        Node l;
        /** END SEARCH VARIABLES **/

        newNode = new Node<>(Illegal_facade, Illegal_facade);
        Facade_Nova.WriteFast(SrzK, key, Facade_Nova.AllocateSlice(newNode, Facade_long_offset_key, 
        		Illegal_facade, SrzK.calculateSize(key), idx),idx);
        Facade_Nova.WriteFast(SrzK, key, Facade_Nova.AllocateSlice(newNode, Facade_long_offset_value,
        		Illegal_facade, SrzV.calculateSize(value), idx),idx);
        		
        try {
            while (true) {

                /** SEARCH **/
                p = root;
                pinfo = p.info;
                l = p.left;
                while (l.left != null) {
                    p = l;
                    l = (l.key == Illegal_facade || Facade_Nova.Compare(key, KCt, l.key) > 0) ? l.left : l.right;
                }
                pinfo = p.info;                             // read pinfo once instead of every iteration
                if (l != p.left && l != p.right) continue;  // then confirm the child link to l is valid
                                                            // (just as if we'd read p's info field before the reference to l)
                /** END SEARCH **/

                if (!(pinfo == null || pinfo.getClass() == Clean.class)) {
                    help(pinfo, idx);
                } else {
                    if (l.key != Illegal_facade && Facade_Nova.Compare(key, KCt, l.key) == 0) {
                        // key already in the tree, try to replace the old node with new node
                        newPInfo = new IInfo(l, p, newNode);
                        result = l.value;
                    } else {
                        // key is not in the tree, try to replace a leaf with a small subtree
                        newSibling = new Node(l.key, l.value);
                        if (l.key == Illegal_facade || Facade_Nova.Compare(key, KCt, l.key) > 0) // newinternal = max(ret.l.key, key);
                        {
                            newInternal = new Node(l.key, newNode, newSibling);
                        } else {
                            newInternal = new Node(Illegal_facade,newSibling,newNode);
                            		Facade_Nova.
                            		WriteFast(SrzK, key,
                            				Facade_Nova.
                            				AllocateSlice(newInternal, Facade_long_offset_key, Illegal_facade, SrzK.calculateSize(key), idx),
                            				idx);
                        }

                        newPInfo = new IInfo(l, p, newInternal);
                        result = Illegal_facade;
                    }

                    // try to IFlag parent
                    if (infoUpdater.compareAndSet(p, pinfo, newPInfo)) {
                        helpInsert(newPInfo);
                        if(result == Illegal_facade) return null;
                        //return null;
                        return (V)Facade_Nova.Read(SrzV, l.value);
                    } else {
                        // if fails, help the current operation
                        // need to get the latest p.info since CAS doesnt return current value
                 	   help(p.info, idx);
                        }
                        return null;
                    }
                }
        }catch (Exception e) { return null;}   
    }

    // Delete key from dictionary, return the associated value when successful, null otherwise
    /** PRECONDITION: k CANNOT BE NULL **/
    public final boolean remove(final K key, int idx){
    	del_count ++;
        /** SEARCH VARIABLES **/
        Node gp;
        Info gpinfo;
        Node p;
        Info pinfo;
        Node l;
        /** END SEARCH VARIABLES **/
        try {
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
                    l = (l.key == Illegal_facade || Facade_Nova.Compare(key, KCt, l.key) > 0 ) ? l.left : l.right;
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
                
                if (l.key == Illegal_facade || Facade_Nova.Compare(key, KCt, l.key)  != 0) return false;
                if (!(gpinfo == null || gpinfo.getClass() == Clean.class)) {
                    help(gpinfo, idx);
                } else if (!(pinfo == null || pinfo.getClass() == Clean.class)) {
                    help(pinfo, idx);
                } else {
                    // try to DFlag grandparent
                    final DInfo newGPInfo = new DInfo(l, p, gp, pinfo);

                    if (infoUpdater.compareAndSet(gp, gpinfo, newGPInfo)) {
                        if (helpDelete(newGPInfo, idx))  {
                        	return true;
                        }
                    } else {
                        // if fails, help grandparent with its latest info value
                        help(gp.info, idx);
                    }
                }
            }
        }catch (Exception e) { return false;}
    }

 //--------------------------------------------------------------------------------
 //PRIVATE METHODS
 //- helpInsert
 //- helpDelete
 //--------------------------------------------------------------------------------

    private void helpInsert(final IInfo info){
        (info.p.left == info.l ? leftUpdater : rightUpdater).compareAndSet(info.p, info.l, info.newInternal);
        infoUpdater.compareAndSet(info.p, info, new Clean());
    }

    private boolean helpDelete(final DInfo info, int idx){
        final boolean result;

        result = infoUpdater.compareAndSet(info.p, info.pinfo, new Mark(info));
        final Info currentPInfo = info.p.info;
        // if  CAS succeed or somebody else already suceed helping, the helpMarked
        if (result || (currentPInfo.getClass() == Mark.class && ((Mark) currentPInfo).dinfo == info)) {
            helpMarked(info, idx);
            return true;
        } else {
            help(currentPInfo, idx);
            infoUpdater.compareAndSet(info.gp, info, new Clean());
            return false;
        }
    }

    private void help(final Info info, int idx) {
        if (info.getClass() == IInfo.class)     helpInsert((IInfo) info);
        else if(info.getClass() == DInfo.class) helpDelete((DInfo) info, idx);
        else if(info.getClass() == Mark.class)  helpMarked(((Mark)info).dinfo,idx);
    }

    private void helpMarked(final DInfo info, int idx) {
        final Node other = (info.p.right == info.l) ? info.p.left : info.p.right;
        (info.gp.left == info.p ? leftUpdater : rightUpdater).compareAndSet(info.gp, info.p, other);
        Facade_Nova.Delete(idx, info.l.key, info.l, Facade_long_offset_key );
        Facade_Nova.Delete(idx, info.l.value, info.l, Facade_long_offset_value );
        Facade_Nova.Delete(idx, info.l.key, info.l, Facade_long_offset_key );
        if(info.p.key != Illegal_facade ) Facade_Nova.Delete(idx, info.p.key, info.p, Facade_long_offset_key );
        infoUpdater.compareAndSet(info.gp, info, new Clean());
    }

   public void Print() {
	       Node l = root.left;
	       PrintAux(root.left);
	       System.out.print("****\n");
   }
   public void PrintAux(Node key) {
	   try {
	       if ( key.left != null) {
	    	   System.out.print("left ");
	    	   PrintAux(key.left);
	       }
	       else
	    	   System.out.print("*no left *");
	       Facade_Nova.Print(KCt, key.key);
	       if (key.right != null) {
	    	   System.out.print("right ");
	    	   PrintAux(key.right);
	       }	       else
	    	   System.out.print("*no right *");
	   		}catch (Exception e) {
	   }
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
       if (node.left == null && node.key != Illegal_facade) return 1;
       return sequentialSize(node.left) + sequentialSize(node.right);
   }
}
