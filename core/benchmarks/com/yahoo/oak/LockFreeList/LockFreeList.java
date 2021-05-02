package com.yahoo.oak.LockFreeList;

import java.util.concurrent.atomic.*;

import com.yahoo.oak.Facade;
import com.yahoo.oak.NovaManager;

class Node {
	AtomicMarkableReference<Node> next;
	Facade value;
	
	public Node(Facade value, Node next) {
		this.next = new AtomicMarkableReference<Node>(next, false);
		this.value = value;
	}
}
	
public class LockFreeList {
	NovaManager novaManager;
	AtomicMarkableReference<Node> head;

	public LockFreeList(NovaManager novaManager) {
		Node headNode = new Node(null, null);
		head = new AtomicMarkableReference<Node>(headNode, false);
	}
	
	public void addFirst(MyBuffer value) {
		addAfter(head.getReference().value, value);
	}
	
	public boolean addAfter(Facade after, MyBuffer value) {
		boolean sucessful = false;
		Facade newF = new Facade(novaManager);
		newF.AllocateSlice(8, 0);
		newF.Write(value, 0);
		while (!sucessful) {
			boolean found = false;
			for (Node node = head.getReference(); node != null && !isRemoved(node); 
			node = node.next.getReference()) {
				if (isEqual(node.value, after) && !node.next.isMarked()) {
					found = true;
					Node nextNode = node.next.getReference();
					Node newNode = new Node(newF, nextNode);
					sucessful = node.next.compareAndSet(nextNode, newNode, false, false);
					break;
				}
			}
			if (!found) {
				return false;
			}
		}
		return true;
	}
	
	public boolean remove(MyBuffer value) {
		boolean sucessful = false;
		while (!sucessful) {
			boolean found = false;
			for (Node node = head.getReference(), nextNode = node.next.getReference();
			nextNode != null; node = nextNode, nextNode = nextNode.next.getReference()) {
				if (!isRemoved(nextNode) && isEqual(nextNode.value, value)) {
					found = true;
					logicallyRemove(nextNode);
					sucessful = physicallyRemove(node, nextNode);
					break;
				}
			}
			if (!found) {
				return false;
			}
		}
		return true;
	}
	
	void logicallyRemove(Node node) {
		while (!node.next.attemptMark(node.next.getReference(), true)) { }
	}
	
	boolean physicallyRemove(Node leftNode, Node node) {
		Node rightNode = node;
		do {
			rightNode = rightNode.next.getReference();
		} while (rightNode != null && isRemoved(rightNode));
		return leftNode.next.compareAndSet(node, rightNode, false, false);
	}
	
	boolean isRemoved(Node node) {
		return node.next.isMarked();
	}

	
	boolean isEqual(Facade arg0, Facade arg1) {
		if (arg0 == null) {
			return arg0 == arg1;
		} else {
			return arg0.equals(arg1);
		}
	}


	public void print() {
		System.out.println("result:");
		for (Node node = head.getReference().next.getReference(); node != null;
		node = node.next.getReference()) {
			System.out.println(node.value);
		}

	}
}
