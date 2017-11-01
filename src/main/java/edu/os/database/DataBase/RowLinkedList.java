package edu.os.database.DataBase;

import java.lang.ref.WeakReference;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class RowLinkedList implements List{
	protected Node head = new Node(null);
	private int size = 0;
	//private HashMap<Row, Integer> numberIndex; 
	private final ReentrantReadWriteLock RWL = new ReentrantReadWriteLock(true);
	
	
	
	public RowLinkedList(){
		head.next = head;
		head.prev = head;
	}
	
	public RowLinkedList(Row[] rows){
		for(Row row : rows){
			add(row);
		}
	}
	/**
	 * ONLY FOR USE IN TABLE CREATED BY SINGLE THREAD. not safe for multi-threading
	 
	void indexNodes(Row row){
		numberIndex = new HashMap<Row, Integer>(size);
		Node current = head.next;
		int counter = 0;
		while (current != head){
			
			numberIndex.put
			
			counter++;
			current = current.next;
		}
	}*/
	

	
	public void readLock(){
		RWL.readLock().lock();
	}
	
	public void readUnlock(){
		RWL.readLock().unlock();
	}
	
	public void writeLock(){
		RWL.writeLock().lock();
	}
	
	public void writeUnlock(){
		RWL.writeLock().unlock();
	}
	
	Node getHead(){
		return head;
	}
	static class Node{
		private Row strongRow;
		private WeakReference<Row> cacheRow = null;
		Node next;
		Node prev;
		
		public Node(Row r){
			strongRow = r;
		}
		
		void setPrevious(Node node){
			prev = node;
		}
		
		Node getPrevious(){
			return prev;
		}
		
		void setNext(Node node){
			next = node;
		}
		
		Node getNext(){
			return next;
		}
		
		public Row row(){//return the weak reference to the row that this node contains, because it will be in cache
			if(cacheRow != null){
				Row tempRow = cacheRow.get();//need to store it in temp variable, incase it falls out of cache after the .get() check
				if(tempRow != null){
					return tempRow;
				}
			}
			return strongRow;
		}
		
		public void cacheRow(Row row){
			cacheRow = new WeakReference<Row>(row);
		}
		
		public void replace(int index, Object newAddition){
			if(cacheRow != null){
				Row temp = cacheRow.get();
				if(temp != null){
					temp.replace(index, newAddition);
				}
			}
			strongRow.replace(index, newAddition);
		}
		
		
	}
	
	public void removeNode(Node node){
		
		node.prev.next = node.next;
		node.next.prev = node.prev;
		size--;
	}
	
	public Node getNode(Row row) {
		RWL.readLock().lock();
		Node result = head.next;
		
		while(result != head){
			if(result.row().equals(row)){
				RWL.readLock().unlock();
				return result;
			}

			result = result.next;
		}
		RWL.readLock().unlock();
		return null;
	}
	
	public Node getNode(int index){
		RWL.readLock().lock();
		Node result = head.next;
		int count = 0;
		while(result != head){
			if(count == index){
				RWL.readLock().unlock();
				return result;
			}
			count++;
			result = result.next;
		}
		RWL.readLock().unlock();
		return null;
	}
	
	public Row deleteRow(int index){
		RWL.readLock().lock();
		int count = 0;
		Node current = head;
		while(current != head){
			if(count == index){
				RWL.writeLock().lock();
				current.row().writeLock();
				current.next.prev = current.prev;
				current.prev.next = current.next;
				size--;
				
				current.row().writeUnlock();
				RWL.writeLock().unlock();
				return current.row();
				
			}
			count++;
			current = current.next;
		}
		RWL.readLock().unlock();
		return null;
	}
	
	public Row getRow(int index){
		return getNode(index).row();
	}
	
	public boolean add(Node node){
		RWL.readLock().lock();
		head.prev.next = node;
		node.prev = head.prev;
		head.prev = node;
		node.next = head;
		
		
		size++;
		RWL.readLock().unlock();
		return true;
	}
	
	
	public boolean add(Row r){
		return add(new Node(r));
	}

	

	@Override
	public void add(int index, Object element) {
		Node node = (Node) element;
		add(node);
		
	}

	@Override
	public boolean addAll(Collection c) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean addAll(int index, Collection c) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void clear() {
		head.prev = head;
		head.next = head;
		
	}

	@Override
	public boolean contains(Object o) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean containsAll(Collection c) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Object get(int index) {
		return getRow(index);

	}

	@Override
	public int indexOf(Object o) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean isEmpty() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Iterator iterator() {
		return new Iterator() {
			private int count;
			private Node current = head;

			@Override
			public boolean hasNext() {
				return current.next != head;
			}

			@Override
			public Object next() {
				current = current.next;
				return current.row();
			}
		};
	}

	@Override
	public int lastIndexOf(Object o) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public ListIterator listIterator() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ListIterator listIterator(int index) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean remove(Object o) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Object remove(int index) {
		return deleteRow(index);
	}

	@Override
	public boolean removeAll(Collection c) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean retainAll(Collection c) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Object set(int index, Object element) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int size() {
		return size;

	}

	@Override
	public List subList(int fromIndex, int toIndex) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object[] toArray() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object[] toArray(Object[] a) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean add(Object e) {
		return add((Row)e);
	}

}

