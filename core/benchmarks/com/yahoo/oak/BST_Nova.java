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

public class BST_Nova<K , V> {
	   final NovaSerializer<K> SrzK;
	   final NovaSerializer<V> SrzV;
	   final NovaC<K> KCt;
	   final NovaC<V> VCt;
   //--------------------------------------------------------------------------------
   // Class: Node
   //--------------------------------------------------------------------------------
   protected final static class Node<E , V> {
       final Facade key;
       final Facade value;
       volatile Node<Facade,Facade>  left;
       volatile Node<Facade,Facade>  right;
       volatile Info<Facade,Facade> info;

       /** FOR MANUAL CREATION OF NODES (only used directly by testbed) **/
       Node(final Facade key, final Facade value,
    		   	final Node left, final Node  right) {
           this.key = key;
           this.value = value;
           this.left = left;
           this.right = right;
           this.info = null;
       }

       /** TO CREATE A LEAF NODE **/
       Node(final Facade key, final Facade value) {
           this(key, value, null, null);
       }

       /** TO CREATE AN INTERNAL NODE **/
       Node(final Facade key, final Node<Facade,Facade> left, final Node<Facade,Facade> right) {
           this(key, null, left, right);
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
       final Node<Facade,Facade> p;
       final Node<Facade,Facade> l;
       final Node<Facade,Facade> gp;
       final Info<Facade,Facade> pinfo;

       DInfo(final Node<Facade,Facade> leaf, final Node<Facade,Facade> parent,
    		   final Node<Facade,Facade> grandparent, final Info<Facade,Facade> pinfo) {
           this.p = parent;
           this.l = leaf;
           this.gp = grandparent;
           this.pinfo = pinfo;
       }
   }

   protected final static class IInfo<E, V> extends Info<E,V> {
       final Node<Facade,Facade> p;
       final Node<Facade,Facade> l;
       final Node<Facade,Facade> newInternal;

       IInfo(final Node<Facade,Facade> leaf, final Node<Facade,Facade> parent,
    		   final Node<Facade,Facade> newInternal){
           this.p = parent;
           this.l = leaf;
           this.newInternal = newInternal;
       }
   }

   protected final static class Mark<E , V> extends Info<E,V> {
       final DInfo<Facade,Facade> dinfo;

       Mark(final DInfo<Facade,Facade> dinfo) {
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

    final Node<Facade,Facade> root;

    public BST_Nova(  NovaSerializer<K> sK, NovaSerializer<V> sV,
 		   			NovaC<K> cKt, NovaC<V> cVt, NovaManager mng) {
        // to avoid handling special case when <= 2 nodes,
        // create 2 dummy nodes, both contain key null
        // All real keys inside BST are required to be non-null
 	   Facade<K> dummyK  = new Facade<K>(mng);
 	   Facade<V> dummyV  = new Facade<V>(mng); //setting up nova manager

 	   SrzK = sK; SrzV = sV;
 	   KCt = cKt; VCt = cVt;
        root = new Node<Facade, Facade>(null,
     		   new Node<Facade, Facade>(null, null), 
     		   new Node<Facade, Facade>(null, null));
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
 	   try {
 	       if (key == null) throw new NullPointerException();
 	       Node<Facade,Facade> l = root.left;
 	       while (l.left != null) {
 	           l = (l.key == null || l.key.Compare(key,KCt) > 0) ? l.left : l.right;
 	       }
 	       return (l.key != null &&  l.key.Compare(key,KCt) == 0) ? true : false;   
 	   }catch (Exception e) {
 		   return false; //Facade throws
 	   }
    }

    /** PRECONDITION: k CANNOT BE NULL **/
    public final V get(final K key, int tidx) {
 	   try {
 	       if (key == null) throw new NullPointerException();
 	       Node<Facade,Facade> l = root.left;
 	       while (l.left != null) {
 	           l = (l.key == null || l.key.Compare(key,KCt) > 0) ? l.left : l.right;
 	       }
 	       V ret = (l.key != null && l.key.Compare(key,KCt) == 0) ? 
 	    		   (V)l.value.Read(SrzV): null;
 	       return ret;
 	   }catch (Exception e) {
 		   return null; //Facade throws	   
 		   }
 	   }

    // Insert key to dictionary, returns the previous value associated with the specified key,
    // or null if there was no mapping for the key
    /** PRECONDITION: k CANNOT BE NULL **/
    public final V putIfAbsent(final K key, final V value, int idx){
        Node<Facade,Facade> newInternal;
        Node<Facade,Facade> newSibling, newNode;

        /** SEARCH VARIABLES **/
        Node<Facade,Facade> p;
        Info<Facade,Facade> pinfo;
        Node<Facade,Facade>l;
        /** END SEARCH VARIABLES **/
        
        Facade<K> k = new Facade<K>();
        Facade<V> v = new Facade<V>();
        
        k.AllocateSlice(SrzK.calculateSize(key), idx);
        v.AllocateSlice(SrzV.calculateSize(value), idx);
        k.WriteFast(SrzK, key, idx);
        v.WriteFast(SrzV, value, idx);

        newNode = new Node<Facade,Facade>(k, v);

        try {
            while (true) {

                /** SEARCH **/
                p = root;
                pinfo = p.info;
                l = p.left;
                while (l.left != null) {
                    p = l;
                    l = (l.key == null || l.key.Compare(key,KCt) > 0) ? l.left : l.right;
                }
                pinfo = p.info;                             // read pinfo once instead of every iteration
                if (l != p.left && l != p.right) continue;  // then confirm the child link to l is valid
                                                            // (just as if we'd read p's info field before the reference to l)
                /** END SEARCH **/

                if (l.key != null && l.key.Compare(key,KCt) == 0) {
                    return (V)l.value.Read(SrzV);	// key already in the tree, no duplicate allowed
                } else if (!(pinfo == null || pinfo.getClass() == Clean.class)) {
                    help(pinfo, idx);
                } else {
                    newSibling = new Node<Facade,Facade>(l.key, l.value);
                    if (l.key == null ||  l.key.Compare(key,KCt) > 0)	// newinternal = max(ret.l.key, key);
                        newInternal = new Node<Facade,Facade>(l.key, newNode, newSibling);
                    else
                        newInternal = new Node<Facade,Facade>(k, newSibling, newNode);

                    final IInfo<Facade,Facade> newPInfo = new IInfo<Facade,Facade>(l, p, newInternal);

                    // try to IFlag parent
                    if (infoUpdater.compareAndSet(p, pinfo, newPInfo)) {
                        helpInsert(newPInfo);
                        return null;
                    } else {
                        // if fails, help the current operation
                        // [CHECK]
                        // need to get the latest p.info since CAS doesnt return current value
                        help(p.info, idx);
                    }
                }
            }   
        }catch (Exception e) {
 		return null; // in case of facade throws
 	}
    }

    // Insert key to dictionary, return the previous value associated with the specified key,
    // or null if there was no mapping for the key
    /** PRECONDITION: k CANNOT BE NULL **/
    public final V put(final K key, final V value, int idx) {
        Node<Facade,Facade> newInternal;
        Node<Facade,Facade> newSibling, newNode;
        IInfo<Facade,Facade> newPInfo;
        Facade<V> result;

        /** SEARCH VARIABLES **/
        Node<Facade,Facade> p;
        Info<Facade,Facade> pinfo;
        Node<Facade,Facade> l;
        /** END SEARCH VARIABLES **/
        Facade<K> k = new Facade<K>();
        Facade<V> v = new Facade<V>();
        
        k.AllocateSlice(SrzK.calculateSize(key), idx);
        v.AllocateSlice(SrzV.calculateSize(value), idx);
        k.WriteFast(SrzK, key, idx);
        v.WriteFast(SrzV, value, idx);
        
        newNode = new Node<Facade,Facade>(k, v);
        try {
            while (true) {

                /** SEARCH **/
                p = root;
                pinfo = p.info;
                l = p.left;
                while (l.left != null) {
                    p = l;
                    l = (l.key == null || l.key.Compare(key,KCt) > 0) ? l.left : l.right;
                }
                pinfo = p.info;                             // read pinfo once instead of every iteration
                if (l != p.left && l != p.right) continue;  // then confirm the child link to l is valid
                                                            // (just as if we'd read p's info field before the reference to l)
                /** END SEARCH **/

                if (!(pinfo == null || pinfo.getClass() == Clean.class)) {
                    help(pinfo, idx);
                } else {
                    if (l.key != null && l.key.Compare(key,KCt) == 0) {
                        // key already in the tree, try to replace the old node with new node
                        newPInfo = new IInfo<Facade,Facade>(l, p, newNode);
                        result = l.value;
                    } else {
                        // key is not in the tree, try to replace a leaf with a small subtree
                        newSibling = new Node<Facade,Facade>(l.key, l.value);
                        if (l.key == null || l.key.Compare(key,KCt) > 0) // newinternal = max(ret.l.key, key);
                        {
                            newInternal = new Node<Facade,Facade>(l.key, newNode, newSibling);
                        } else {
                            Facade<K> tmp = new Facade<K>();
                            tmp.AllocateSlice(SrzK.calculateSize(key), idx);
                            tmp.WriteFast(SrzK, key, idx);
                            newInternal = new Node<Facade,Facade>(tmp, newSibling, newNode);
                        }

                        newPInfo = new IInfo<Facade,Facade>(l, p, newInternal);
                        result = null;
                    }

                    // try to IFlag parent
                    if (infoUpdater.compareAndSet(p, pinfo, newPInfo)) {
                        helpInsert(newPInfo);
                        if(result == null) return null;
                        //return null;
                        return (V)result.Read(SrzV);
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

        /** SEARCH VARIABLES **/
        Node<Facade,Facade> gp;
        Info<Facade,Facade> gpinfo;
        Node<Facade,Facade> p;
        Info<Facade,Facade> pinfo;
        Node<Facade,Facade> l;
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
                    l = (l.key == null || l.key.Compare(key,KCt) > 0 ) ? l.left : l.right;
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
                
                if (l.key == null || l.key.Compare(key,KCt) != 0) return false;
                if (!(gpinfo == null || gpinfo.getClass() == Clean.class)) {
                    help(gpinfo, idx);
                } else if (!(pinfo == null || pinfo.getClass() == Clean.class)) {
                    help(pinfo, idx);
                } else {
                    // try to DFlag grandparent
                    final DInfo<Facade,Facade> newGPInfo = new DInfo<Facade,Facade>(l, p, gp, pinfo);

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

    private void helpInsert(final IInfo<Facade,Facade> info){
        (info.p.left == info.l ? leftUpdater : rightUpdater).compareAndSet(info.p, info.l, info.newInternal);
        infoUpdater.compareAndSet(info.p, info, new Clean());
    }

    private boolean helpDelete(final DInfo<Facade,Facade> info, int idx){
        final boolean result;

        result = infoUpdater.compareAndSet(info.p, info.pinfo, new Mark<Facade,Facade>(info));
        final Info<Facade,Facade> currentPInfo = info.p.info;
        // if  CAS succeed or somebody else already suceed helping, the helpMarked
        if (result || (currentPInfo.getClass() == Mark.class && ((Mark<Facade,Facade>) currentPInfo).dinfo == info)) {
            helpMarked(info, idx);
            return true;
        } else {
            help(currentPInfo, idx);
            infoUpdater.compareAndSet(info.gp, info, new Clean());
            return false;
        }
    }

    private void help(final Info<Facade,Facade> info, int idx) {
        if (info.getClass() == IInfo.class)     helpInsert((IInfo<Facade,Facade>) info);
        else if(info.getClass() == DInfo.class) helpDelete((DInfo<Facade,Facade>) info, idx);
        else if(info.getClass() == Mark.class)  helpMarked(((Mark<Facade,Facade>)info).dinfo,idx);
    }

    private void helpMarked(final DInfo<Facade,Facade> info, int idx) {
        final Node<Facade,Facade> other = (info.p.right == info.l) ? info.p.left : info.p.right;
        (info.gp.left == info.p ? leftUpdater : rightUpdater).compareAndSet(info.gp, info.p, other);
        info.l.key.Delete(idx);
        info.l.value.Delete(idx);
        if(info.p.key != null )info.p.key.Delete(idx);
        infoUpdater.compareAndSet(info.gp, info, new Clean());
    }

   public void Print() {
	       Node<Facade,Facade> l = root.left;
	       PrintAux(root.left);
	       System.out.print("****\n");
   }
   public void PrintAux(Node key) {
	   try {
	       if ( key.left != null) {
	    	   System.out.print("left ");
	    	   PrintAux(key.left);
	       }
	       key.key.Print(KCt);
	       if (key.right != null) {
	    	   System.out.print("right ");
	    	   PrintAux(key.right);
	       }
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
       if (node.left == null && node.key != null) return 1;
       return sequentialSize(node.left) + sequentialSize(node.right);
   }
}
