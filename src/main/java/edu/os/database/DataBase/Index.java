package edu.os.database.DataBase;

import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import edu.yu.cs.dataStructures.fall2016.SimpleSQLParser.BTree;
import edu.yu.cs.dataStructures.fall2016.SimpleSQLParser.ColumnDescription;

public class Index {
	BTree index = null;
	ColumnDescription.DataType type;
	ColumnDescription column;
	ReentrantReadWriteLock RWL = new ReentrantReadWriteLock(true);
	int columnNumber;
	
	public Index(ColumnDescription column, int columnNum){
		type = column.getColumnType();
		this.column = column;
		columnNumber = columnNum;
		if(type == ColumnDescription.DataType.VARCHAR)
			index = new BTree<String, ArrayList<RowLinkedList.Node>>();
			
		else if(type == ColumnDescription.DataType.INT)
			index = new BTree<Integer, ArrayList<RowLinkedList.Node>>();
		
		else if(type == ColumnDescription.DataType.DECIMAL)
			index = new BTree<Integer, ArrayList<RowLinkedList.Node>>();
	}
	
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
	
	public void put(RowLinkedList.Node node){
		Comparable key =  (Comparable) node.row().getObjectAt(columnNumber);
		ArrayList<RowLinkedList.Node> value = (ArrayList<RowLinkedList.Node>)index.get(key);
		if(value == null){
			value = new ArrayList<RowLinkedList.Node>();
			value.add(node);
			index.put(key, value);
		}
		else{
			value.add(node);
		}
	}
	
	public ArrayList<RowLinkedList.Node> get(Comparable value){
		return (ArrayList<RowLinkedList.Node>) index.get(value);
	}
	
	public boolean isInitialized(){
		return index != null;
	}
}
