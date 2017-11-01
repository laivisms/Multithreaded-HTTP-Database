package edu.os.database.DataBase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObjectBuilder;

import edu.yu.cs.dataStructures.fall2016.SimpleSQLParser.ColumnDescription;
import edu.yu.cs.dataStructures.fall2016.SimpleSQLParser.ColumnValuePair;
import edu.yu.cs.dataStructures.fall2016.SimpleSQLParser.CreateTableQuery;
import edu.yu.cs.dataStructures.fall2016.SimpleSQLParser.InsertQuery;


public class Table {
	private int rowCount = 0;
	private String name = null;
	private String OriginalQuery = null;
	private boolean persistant = true; //should the table to written to memory or not
	//private int editCount = 0;
	private String primary;
	private Index[] indexes;
	private ColumnDescription[] columns;
	private ArrayList<String> columnNames = new ArrayList<String>();
	private HashMap<String, Integer> colIndex;
	private final ReentrantReadWriteLock tableLock = new ReentrantReadWriteLock(true);
	private final ArrayList<ReentrantReadWriteLock> columnRWL = new ArrayList<ReentrantReadWriteLock>();
	private ArrayList<RowLinkedList> subRows = new ArrayList<RowLinkedList>();//only for use in temp tables created by result sets, by one thread
	private List<Row> rows = new RowLinkedList();
	private ArrayList<Integer> stringLocations = null;
	HashSet<Object>[] uniques;
	
	
	public Table(CreateTableQuery template){
		name = template.getTableName();
		columns = template.getColumnDescriptions();
		int colLength = columns.length;
		uniques = new HashSet[colLength];
		for(ColumnDescription column : columns){
			columnNames.add(column.getColumnName());
			column.setTableName(name);
		}
		primary = template.getPrimaryKeyColumn().getColumnName();
		indexes = new Index[colLength];
		addColLocks(colLength);
		colIndex = new HashMap<String, Integer>(colLength);
		for(int i=0; i<colLength; i++){
			colIndex.put(columns[i].getColumnName(), i);// associates all column names with their indexes, for fast lookup
			if(columns[i].isUnique() || columns[i].getColumnName().equals(primary)){
				uniques[i] = new HashSet<Object>();
			}
		}
		createIndex(colIndex.get(primary));
		OriginalQuery = template.getQueryString();
		initializeStringLocations();
		
	}
	
	void initializeStringLocations(){
		if(stringLocations == null){
			stringLocations = new ArrayList<Integer>();
			for(int i=0; i<columns.length; i++){
				if(columns[i].getColumnType().toString().equals("VARCHAR")){
					stringLocations.add(i);
				}
			}
		}
	}
	
	Table(ColumnDescription[] columnD, String name){
		this.name = name;
		columns = columnD;
		int colLength = columns.length;
		addColLocks(colLength);
		colIndex = new HashMap<String, Integer>(colLength);
		for(int i=0; i<colLength; i++){
			colIndex.put(columns[i].getColumnName(), i);
			columnNames.add(columns[i].getColumnName());
		}
		persistant = false;
	}
	/*
	 * Only for use in temporary tables made by the conditionHandler when processing request
	 */
	void addSubRows(RowLinkedList rows){
		subRows.add(rows);
	}
	
	ArrayList<RowLinkedList> getSubRows(){
		return subRows;
	}
	
	public void readLock(){
		tableLock.readLock().lock();
	}
	
	public void readUnlock(){
		tableLock.readLock().unlock();
	}
	
	public void writeLock(){
		tableLock.writeLock().lock();
	}
	
	public void writeUnlock(){
		tableLock.writeLock().unlock();
	}
	
	private void createIndex(int colNumber){
		
		Index result =  new Index(columns[colNumber], colNumber);
		if(result.isInitialized())
			indexes[colNumber] = result;
		
	}
	
	private void addColLocks(int colCount){
		for(int i=0;i<colCount;i++){
			columnRWL.add(new ReentrantReadWriteLock(true));
		}
	}
	
	
	public void addRow(InsertQuery query) throws NullEntryException, DataTypeMismatchException, DuplicateEntryException, InvalidLengthException{
		
		ColumnValuePair[] pairs = query.getColumnValuePairs();
		int pairLength = pairs.length;
		Row temp = new Row(columns.length, name);
		for (int i=0; i<pairLength;i++) { //iterate over every pair
			ColumnValuePair pair = pairs[i];
			int index = colIndex.get(pair.getColumnID().getColumnName());
			ColumnDescription column = columns[index];
			String inputValue = pair.getValue();
			
			parseEntry(inputValue, temp, column);
			
			//at this point, since no exceptions thrown, know that entry is non-null and correct type.
		}
		
		int rowSize = columns.length;
		RowLinkedList.Node tempNode = new RowLinkedList.Node(temp);
		
		for (int i=0; i< rowSize; i++){// check all columns, ensuring that they are not null, setting default, and indexes where required
			if (temp.getObjectAt(i) == null){
				if(i == colIndex.get(primary)){ // if this is primary key column, cannot be null or duplicate
					throw new NullEntryException(columns[i].getColumnName());
				}
				if(columns[i].getHasDefault()){
					parseEntry(columns[i].getDefaultValue(), temp, columns[i]);
				}
				
				if(columns[i].isNotNull() && temp.getObjectAt(i) == null){//recheck for nullity instead of using else-if in case the default value was null
					throw new NullEntryException(columns[i].getColumnName());
				}
					
			}
		}
		//now that we know the row is OK to insert, iterate over the columns one last time to insert columns into indexes and unique set, where appropriate.
		for(int i=0; i<rowSize; i++){
			addToIndex(i, tempNode);
			if(uniques[i] != null){
				uniques[i].add(tempNode.row().getObjectAt(i));
			}
		}
		
		((RowLinkedList)rows).add(tempNode);
		rowCount++;
		
	}
	
	public void addRow(Row row) throws DataTypeMismatchException{
		int rowSize = row.getSize();
		for(int i=0; i<rowSize; i++){
			ColumnDescription current = columns[i];
			if(!isMatchingType(current.getColumnName(), row.getObjectAt(i))){
				throw new DataTypeMismatchException(current.getColumnName(), current.getColumnType().toString());
			}
		}
		rows.add(row);
	}
	
	private void parseEntry(String inputValue, Row temp, ColumnDescription column) throws NullEntryException, DataTypeMismatchException, DuplicateEntryException, InvalidLengthException{
		
		String columnName = column.getColumnName();
		int index = colIndex.get(columnName);
		
		
		if ((inputValue == null || inputValue.equals("NULL"))  && column.isNotNull()){
			throw new NullEntryException(columnName);
		}
		
		if((inputValue == null || inputValue.equals("NULL"))  && !column.isNotNull()){
			temp.replace(index, null);
		}
		
		else if(isInt(inputValue)) {
			if(column.getColumnType() != ColumnDescription.DataType.INT){
				throw new DataTypeMismatchException(columnName, "Integer");
			}
			temp.replace(index, Integer.parseInt(inputValue));
		}
		
		else if(isDecimal(inputValue)){
			if(column.getColumnType() != ColumnDescription.DataType.DECIMAL){
				throw new DataTypeMismatchException(columnName, "Decimal");
			}
			int wholeLength = inputValue.indexOf('.');
			int fractionLength = inputValue.length() - 1 - wholeLength;
			if(column.getFractionLength() < fractionLength || column.getWholeNumberLength() < wholeLength){
				throw new InvalidLengthException("Decimal", columnName);
			}
			temp.replace(index, Float.parseFloat(inputValue));
		}
		
		else if(isBoolean(inputValue)){
			if(column.getColumnType() != ColumnDescription.DataType.BOOLEAN){
				throw new DataTypeMismatchException(columnName, "Boolean");
			}
			temp.replace(index, parseBoolean(inputValue));
		}
		
		else{//string
			if(column.getColumnType() != ColumnDescription.DataType.VARCHAR){
				throw new DataTypeMismatchException(columnName, "String");
			}
			
			if(column.getVarCharLength() < inputValue.length()){
				throw new InvalidLengthException("Varchar", columnName);
			}
			inputValue = inputValue.substring(1, inputValue.length() - 1);// remove quotation marks
			temp.replace(index, inputValue);
		}
		if(column.isUnique() || column.getColumnName().equals(primary)){
			if(isDuplicate(index, temp.getObjectAt(index))){
				throw new DuplicateEntryException(column.getColumnName());
			}
		}
	}
	
	private boolean isInt(String integer){
		boolean result = true;
		try{
			Integer.parseInt(integer);
		}
		catch(NumberFormatException e){
			result = false;
		}
		return result;
	}
	
	private boolean isDecimal(String decimal){
		boolean result = true;
		try{
			Float.parseFloat(decimal);
		}
		catch(NumberFormatException e){
			result = false;
		}
		
		return result;
	}
	
	private boolean isBoolean(String Boolean){
		if(Boolean.toLowerCase().equals("false") || Boolean.toLowerCase().equals("true")){
			return true;
		}
		return false;
	}
	
	/*
	 * returns false for null input
	 */
	private boolean parseBoolean(String Boolean){
		if (Boolean.toLowerCase().equals("true")){
			return true;
		}
		else{
			return false;
		}
	}
	
	/**
	 * 
	 * @param column the column which cannot contain obj
	 * @param obj The obj which must not already be in column
	 * @return true if object already in column, false otherwise
	 */
	private boolean isDuplicate(int column, Object obj){
		
		//return uniques[column].contains(obj);
		return false;
	}
	/*
	 * finds the rows node, then indexes it. used only when indexing a new column
	 */
	private void addToIndex(int column, Row row){
		addToIndex(column, ((RowLinkedList)rows).getNode(row));
	}
	
	private void addToIndex(int column, RowLinkedList.Node tempNode){
		if( isIndexed(column) && isMatchingType(columns[column].getColumnName(), tempNode.row().getObjectAt(column) + "")){
			indexes[column].writeLock();
			indexes[column].put(tempNode);
			indexes[column].writeUnlock();
		}
	}
	private boolean isIndexed(int colNum){
		return indexes[colNum] != null;
	}

	private boolean isMatchingType(String columnName, String value){
		
		ColumnDescription column = columns[colIndex.get(columnName)];
		if(value != null){
			if((column.getColumnType() == ColumnDescription.DataType.INT && !isInt(value)) ||
			  (column.getColumnType() == ColumnDescription.DataType.DECIMAL && !isDecimal(value)) ||
			  (column.getColumnType() == ColumnDescription.DataType.BOOLEAN && !isBoolean(value))){
				return false;
			}
		}
		  
		return true;
		
	}

	private boolean isMatchingType(String columnName, Object value){
		
		ColumnDescription column = columns[colIndex.get(columnName)];
		if(value != null){
			if((column.getColumnType() == ColumnDescription.DataType.INT && !(value instanceof Integer)) ||
			  (column.getColumnType() == ColumnDescription.DataType.DECIMAL && !(value instanceof Float)) ||
			  (column.getColumnType() == ColumnDescription.DataType.BOOLEAN && !(value instanceof Boolean)) ||
			  (column.getColumnType() == ColumnDescription.DataType.VARCHAR && !(value instanceof String))){
				return false;
			}
		}
		  
		return true;
		
	}

	/*
	 * requires locking in outer function
	 */
	public boolean indexColumn(String columnName){
		boolean result = false;
		
		int index = colIndex.get(columnName);
		createIndex(index);
		if (isIndexed(index)){
			result = true;
			for(Row row : rows){
				addToIndex(index, row);
			}
		}
		
		return result;
	}
	
	
	public boolean isIndexed(String colName){
		return isIndexed(colIndex.get(colName));
	}
	
	
	public ArrayList<RowLinkedList.Node> getRowsWhere(String columnName, String value){
		int columnIndex = colIndex.get(columnName);
		ColumnDescription column = columns[columnIndex];
		Object toMatch = convertValue(column.getColumnType(), value);
		return getRowsWhere(columnName, toMatch);
	}
	
	public ArrayList<RowLinkedList.Node> getRowsWhere(String columnName, Object toMatch){
		ArrayList<RowLinkedList.Node> result = new ArrayList<RowLinkedList.Node>();
		int columnIndex = colIndex.get(columnName);
		
		if(isMatchingType(columnName, toMatch)){
			if(isIndexed(columnName)){
				result = (ArrayList<RowLinkedList.Node>)indexes[colIndex.get(columnName)].get((Comparable)toMatch);
			}
			else{
				for(Row row : rows){
					
					if(row.getObjectAt(columnIndex) != null && row.getObjectAt(columnIndex).equals(toMatch)){
						result.add(((RowLinkedList)rows).getNode(row));
					}
				}
			
			}
		}
		if (result == null){
			return new ArrayList<RowLinkedList.Node>();
		}
		
		
		return result;
	}
	
	List<Row> getAllRows(){
		return rows;
	}
	
	ListIterator<Row> getRowIterator(){
		return rows.listIterator();
	}
	
	Row testGetRowAt(int index){
		return rows.get(index);
	}
	
	public Object convertValue(ColumnDescription.DataType type, String value){
		if(value.equals("null")){
			return null;
		}
		if(type == ColumnDescription.DataType.INT && isInt(value)){
			return Integer.parseInt(value);
		}
		else if (type == ColumnDescription.DataType.DECIMAL && isDecimal(value)){
			return Float.parseFloat(value);
		}
		else if(type == ColumnDescription.DataType.BOOLEAN && isBoolean(value)){
			if(value.equals("true")){
				return true;
			}
			else{
				return false;
			}
		}
		if(value.substring(0,1).equals("'")){
				value = value.substring(1, value.length());
		}
		if(value.substring(value.length()-1,value.length()).equals("'")){
			value = value.substring(0, value.length()-1);
		}
		return value;
	}
	
	public Row getRowCopy(Object toFind, int col){
		tableLock.readLock().lock();
		columnRWL.get(col).readLock().lock();
		
		
		
		Row copy = null;
		Row currentRow = null;
		for(int i=0;i<rowCount;i++){
			currentRow = rows.get(i);
			currentRow.readLock();
			if(currentRow.getObjectAt(col).equals(toFind)){
				copy = currentRow.getCopy();
				currentRow.readUnlock();
				break;
			}
			currentRow.readUnlock();
		}
		
		
		
		columnRWL.get(col).readLock().unlock();
		tableLock.readLock().unlock();
		
		return copy;
	}
	
	public int getRowCount(){
		return rows.size();
	}
	
	public String getName(){
		return name;
	}
	
	ColumnDescription[] getColumnDescription(){
		return columns;
	}
	
	int getIndex(String column){
		Integer index = null;
		try{
			index =  colIndex.get(column);
		} catch(NullPointerException e){
			return -1;
		}
		return index;
	}
	
	RowLinkedList.Node getHead(){
		return ((RowLinkedList) rows).getHead();
	}
	
	public boolean deleteRow(Row row){
		RowLinkedList.Node head = ((RowLinkedList) rows).getHead();
		RowLinkedList.Node current = head.next;
		Row placeholder = null;// to make sure that the row that were using doesnt disappear in the middle, if in cache
		
		while (current != head){
			placeholder = current.row();
			placeholder.readLock();
			if(placeholder.equals(row)){
				placeholder.readUnlock();
				placeholder.writeLock();
				
				((RowLinkedList)rows).removeNode(current);
				placeholder.writeUnlock();
				for(int i=0; i<uniques.length; i++){
					if(uniques[i] != null){
						uniques[i].remove(placeholder.getObjectAt(i));
					}
				}
				return true;
			}
			placeholder.readUnlock();
			current = current.next;
		}
		
		return false;
	}
	
	
	public void updateRow(Row rowToFind, ColumnValuePair[] pairs) throws DataTypeMismatchException, DuplicateEntryException{
		RowLinkedList.Node node = ((RowLinkedList)rows).getNode(rowToFind);
		int index;
		String columnName;
		ColumnDescription column;
		Row placeholder = node.row();
		placeholder.writeLock();
		
		for(ColumnValuePair pair : pairs){
			columnName = pair.getColumnID().getColumnName();
			index = getIndex(columnName);
			column = columns[index];
			if(!isMatchingType(pair.getColumnID().getColumnName(), pair.getValue())){
				throw new DataTypeMismatchException(pair.getColumnID().getColumnName(), columns[index].getColumnType() + "");
			}
			Comparable oldValue = (Comparable) placeholder.getObjectAt(index);
			Comparable newValue = (Comparable) convertValue(column.getColumnType(), pair.getValue());
			if(uniques[index] != null){//update unique column set, if need to
				if(!oldValue.equals(newValue) && !isDuplicate(index, newValue)){
					placeholder.writeUnlock();
					throw new DuplicateEntryException(columnName);
				}
				uniques[index].remove(oldValue);
				uniques[index].add(placeholder.getObjectAt(index));
			}
			
			node.replace(index, convertValue(column.getColumnType(), pair.getValue()));
			if(isIndexed(pair.getColumnID().getColumnName())){
				ArrayList<RowLinkedList.Node> values = indexes[index].get(oldValue);
				values.remove(node);
				indexes[index].put(node);
			}
		}
		
		placeholder.writeUnlock();
	}
	@Override
	public String toString(){
		
		StringBuilder result = new StringBuilder();
		
		result.append(columnsString());
		initializeStringLocations();
		RowLinkedList.Node head = getHead();
		RowLinkedList.Node current = head.next;
		Row placeholder = null;
		
		while(current != head){
			placeholder = current.row();
			
			placeholder.readLock();
			
			if(current.next == head){
				result.append(placeholder.toString(stringLocations) + "|");
			}else{
				result.append(placeholder.toString(stringLocations) + "|" + System.lineSeparator());
			}
			
			placeholder.readUnlock();
			
			current = current.next;
		}
		return result.toString();
	}
	
	public String columnsString(){
		StringBuilder result = new StringBuilder();
		for(int i=0; i<columns.length; i++){
			
			if(i == columns.length - 1){
				result.append(columns[i].getColumnName()+ "|" + System.lineSeparator());
			}
			else{
				result.append(columns[i].getColumnName() + ", ");
			}
		}
		return result.toString();
	}
	
public String toJson(){
	
		JsonObjectBuilder builder = Json.createObjectBuilder();
		builder.add("Columns", columnsJSON());
		JsonArrayBuilder JRows = Json.createArrayBuilder();
		initializeStringLocations();
		RowLinkedList.Node head = getHead();
		RowLinkedList.Node current = head.next;
		Row placeholder = null;
		
		while(current != head){
			placeholder = current.row();
			
			placeholder.readLock();
			
			JRows.add(placeholder.toJSON(stringLocations));
			
			placeholder.readUnlock();
			
			current = current.next;
		}
		builder.add("Rows", JRows.build());
		return builder.build().toString();
	}
	
	public JsonArray columnsJSON(){
		JsonArrayBuilder builder = Json.createArrayBuilder();
		for(int i=0; i<columns.length; i++){
			builder.add(columns[i].getColumnName());
		}
		return builder.build();
	}
	
	public boolean hasColumn(String column){
		return columnNames.contains(column);
	}
	
	public String getOriginalQuery(){
		return OriginalQuery;
	}
}

