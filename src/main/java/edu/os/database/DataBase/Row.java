package edu.os.database.DataBase;

import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
//import javax.json.JsonObject;
//import javax.json.JsonObjectBuilder;

public class Row {
	private Object[] columns;
	private int head = 0;
	private final ReentrantReadWriteLock RWL = new ReentrantReadWriteLock(true);
	private String tableName = null;
	
	
	public Row(int size, String tableName){
		columns = new Object[size];
		this.tableName = tableName;
	}
	@Override
	public String toString(){
		readLock();
		StringBuilder result = new StringBuilder();
		for(int i=0; i<columns.length; i++){
			if(i == columns.length-1){
				result.append(columns[i]);
			}
			else{
				result.append(columns[i] + ", ");
			}
		}
		readUnlock();
		
		return result.toString();
	}
	
	public JsonArray toJSON(ArrayList<Integer> stringLoc){
		 JsonArrayBuilder builder = Json.createArrayBuilder();
		 for(int i = 0; i<columns.length; i++){
			 if(stringLoc.contains(i)){
				 builder.add("'" + columns[i] + "'");
			}else{
				 builder.add(columns[i] + "");
			}
		 }
		 return builder.build();

	}
	
	public String toString(ArrayList<Integer> stringLoc){
		readLock();
		StringBuilder result = new StringBuilder();
		for(int i=0; i<columns.length; i++){
			if(i == columns.length-1){
				if(stringLoc.contains(i)){
					result.append("'" + columns[i] + "'");
				}else{
					result.append(columns[i]);
				}
			}
			else{
				if(stringLoc.contains(i)){
					result.append("'" + columns[i] + "', ");
				}else{
					result.append(columns[i] + ", ");
				}
			}
		}
		readUnlock();
		
		return result.toString();
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
	
	public void add(Object addition){
		if (isFull()) {
			throw new IllegalStateException("Row is full");
		}
		else {
			columns[head] = addition;
			head++;
		}
	}
	
	public String getTableName(){
		return tableName;
	}
	
	public void setTableName(String name){
		tableName = name;
	}
	
	public boolean isFull(){
		return(head == columns.length);
	}
	
	public void replace(int index, Object newAddition){
		if (index>columns.length){
			throw new IllegalArgumentException("Out of Bounds");
		}
		else {
			columns[index] = newAddition;
		}
	}
	
	public Object getObjectAt(int index){
		if(index<columns.length){
			return columns[index];
		}
		return null;
	}
	
	public Row getCopy(){
		Row copy = new Row(columns.length, tableName);
		copy.replaceArray(columns);
		return copy;
	}
	
	void replaceArray(Object[] array){
		columns = array.clone();
	}
	
	public int getSize(){
		return columns.length;
	}
	/*
	 * concatentate row on to the end of this row and return new Row of the result
	 */
	public Row concat(Row row){
		Row result = new Row(columns.length + row.getSize(), tableName + ", " + row.getTableName());
		for(int i=0; i< columns.length; i++){
			result.replace(i, columns[i]);
		}
		int counter = 0;
		for(int i=columns.length; i<result.getSize(); i++){
			result.replace(i, row.getObjectAt(counter));
			counter++;
		}
		
		return result;
	}
	
	@Override
	public boolean equals(Object obj){
		Row row = (Row) obj;
		
		if (columns.length != row.getSize()){
			return false;
		}
		for (int i=0; i<columns.length; i++){
			if (columns[i] != null && row.getObjectAt(i) != null){
				if((row.getObjectAt(i) != null && columns[i]==null) ||
				   (row.getObjectAt(i) == null && columns[i] == null) ||
				   (!row.getObjectAt(i).equals(columns[i]))){
					return false;
				}
			}
		}
		return true;
		
	}
	
	
}

