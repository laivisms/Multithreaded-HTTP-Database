package edu.os.database.DataBase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

import edu.yu.cs.dataStructures.fall2016.SimpleSQLParser.ColumnDescription;
import edu.yu.cs.dataStructures.fall2016.SimpleSQLParser.ColumnID;
import edu.yu.cs.dataStructures.fall2016.SimpleSQLParser.ColumnValuePair;
import edu.yu.cs.dataStructures.fall2016.SimpleSQLParser.Condition;
import edu.yu.cs.dataStructures.fall2016.SimpleSQLParser.CreateTableQuery;
import edu.yu.cs.dataStructures.fall2016.SimpleSQLParser.DeleteQuery;
import edu.yu.cs.dataStructures.fall2016.SimpleSQLParser.InsertQuery;
import edu.yu.cs.dataStructures.fall2016.SimpleSQLParser.SQLParser;
import edu.yu.cs.dataStructures.fall2016.SimpleSQLParser.SQLQuery;
import edu.yu.cs.dataStructures.fall2016.SimpleSQLParser.SelectQuery;
import edu.yu.cs.dataStructures.fall2016.SimpleSQLParser.SelectQuery.FunctionInstance;
import edu.yu.cs.dataStructures.fall2016.SimpleSQLParser.UpdateQuery;
import net.sf.jsqlparser.JSQLParserException;

public class QueryHandler implements Runnable{
	DataBase DB;
	SQLQuery topQuery;
	DBServer.stringHandle output;
	SQLParser parser = new SQLParser();
	JsonObjectBuilder errorResult = Json.createObjectBuilder();
	/* outPutHandler something; */
	
	public QueryHandler(DataBase DB, String Q, DBServer.stringHandle output){
		this.DB = DB;
		errorResult.add("Result", "Error - DataBase Query in incorrect format.");
		SQLQuery result = null;
		try {
			result = parser.parse(Q);
		} catch (JSQLParserException e) {
			output.output = errorResult.build().toString();
		}
		if(result == null && !Q.toLowerCase().equals("show tables")){
			output.output = errorResult.build().toString();
		}
		else if(result == null && Q.toLowerCase().equals("show tables")){
			output.output = DB.getAllTableNamesJson().toString();
			
		}
		topQuery = result;
		this.output = output;
	}
	
	@Override
	public void run(){
		try{
			if(topQuery instanceof SelectQuery){
					selectQuery();
			}
			
			else if(topQuery instanceof DeleteQuery){
					deleteQuery();
			}
			
			else if(topQuery instanceof InsertQuery){
				
					insertQuery();
			}
			
			else if(topQuery instanceof CreateTableQuery){
				CreateTableQuery createQuery = (CreateTableQuery) topQuery;
				DB.createTable(createQuery);
				output.output = (DB.getTable(createQuery.getTableName())).toJson();
			}
			
			else{
					updateQuery();
			}
		}catch(Exception e){
			output.output = errorResult.add("Result", e.getMessage() + "").build().toString();
		}
			
		
		 
		
	}
	
	private void selectQuery() throws DataTypeMismatchException{
		ArrayList<Row> toCache = new ArrayList<Row>();
		SelectQuery query = (SelectQuery)topQuery;
		String[] tableNames = query.getFromTableNames();
		ColumnID[] columnNames = query.getSelectedColumnNames();
		for(String table : tableNames){
			hasTableAndColumns(table, columnNames);
		}
		
		HashMap<String, Table> tables = new HashMap<String, Table>();
		for(String name : tableNames){
			tables.put(name, DB.getTable(name));
		}
		
		//on every iteration of the where, replace the table in the map with the new table
		Condition where = query.getWhereCondition();
		Table resultTable = null;
		ColumnID[] columns = query.getSelectedColumnNames();
		if(where == null){//no where condition
			resultTable = tables.get(tableNames[0]);
			for (int i=1; i<tableNames.length; i++){
				resultTable = ConditionHandler.cartesianProduct(resultTable, tables.get(tableNames[i]));
			}
			resultTable = trimColumns(resultTable, columns);
			output.output = resultTable.toJson();
			
		} else{
			resultTable = ConditionHandler.parseCondition(where, tables, tableNames, toCache);
			String[] tablesUsed = resultTable.getName().split(",");
		
			for(int i=0; i<tableNames.length; i++){// tack on the rest of the tables
				if (!tableWasUsed(tablesUsed, tableNames[i])){
					resultTable = ConditionHandler.cartesianProduct(resultTable, tables.get(tableNames[i]));
				}
			}
			resultTable = trimColumns(resultTable, columns);
			
			output.output = resultTable.toJson();
		}
		Table functionTable = resultTable;
		Map<ColumnID, FunctionInstance> functions = query.getFunctionMap();
		String result = null;
		for(ColumnID column : columnNames){
			if(functions.containsKey(column)){
				FunctionInstance FI = functions.get(column);
				if(FI.isDistinct){
					functionTable = makeDistinct(resultTable, column);
				}
				switch(FI.function.toString()){
					case "AVG":
						result += ("AVG(" + column.getColumnName() + ") : " + getAvg(functionTable, column));
						break;
					case "COUNT":
						result += ("COUNT(" + column.getColumnName() + ") : " + functionTable.getAllRows().size());
						break;
					case "MAX":
						result += ("MAX(" + column.getColumnName() + ") : " + getMax(functionTable, column));
						break;
					case "MIN":
						result += ("MIN(" + column.getColumnName() + ") : " + getMin(functionTable, column));
						break;
					case "SUM":
						result += ("SUM(" + column.getColumnName() + ") : " + getSum(functionTable, column));
						break;
				}
			}
		}
		if(result != null){// there was a function performed on the results of the where
			output.output = "{\"Result\" : \"" + result + "\"}";
		}
		DB.addRowsToCache(toCache);
		
		
	}
	
	private void deleteQuery() throws DataTypeMismatchException{
		ArrayList<Row> toCache = new ArrayList<Row>();
		DeleteQuery query = (DeleteQuery)topQuery;
		String[] tableName = {query.getTableName()};
		
		hasTable(query.getTableName());
		
		HashMap<String, Table> tables = new HashMap<String, Table>();
		tables.put(tableName[0], DB.getTable(tableName[0]));
		Condition where = query.getWhereCondition();
		Table resultTable;
		if(where == null){
			resultTable = tables.get(tableName[0]);
		}else{
			resultTable = ConditionHandler.parseCondition(where, tables, tableName, toCache);
		}
		
		RowLinkedList.Node head = resultTable.getHead();
		RowLinkedList.Node current = head.next;
		
		while(current != head){
			
			tables.get(tableName[0]).deleteRow(current.row());
			
			
			current = current.next;
		}
		output.output = (tables.get(tableName[0]).toJson());
		DB.addRowsToCache(toCache);
	}
	
	private void insertQuery() throws NullEntryException, DataTypeMismatchException, DuplicateEntryException, InvalidLengthException{
		InsertQuery query = (InsertQuery)topQuery;
		String tableName = query.getTableName();
		hasTable(tableName);
		Table table = DB.getTable(tableName);
		table.readLock();
		table.addRow(query);
		table.readUnlock();
		if(table != null){
			output.output = (table.toJson());
		}
		DB.addEdits(1);
	}
	
	private void updateQuery() throws DataTypeMismatchException, DuplicateEntryException{
		ArrayList<Row> toCache = new ArrayList<Row>();
		UpdateQuery query = (UpdateQuery)topQuery;
		ColumnValuePair[] pairs = query.getColumnValuePairs();
		hasTable(query.getTableName());
		Table fromTable = DB.getTable(query.getTableName());
		
		
		HashMap<String, Table> tables = new HashMap<String, Table>();
		tables.put(query.getTableName(), fromTable);
		String[] tableName = {query.getTableName()};
		Table resultTable = ConditionHandler.parseCondition(query.getWhereCondition(), tables, tableName, toCache);
		
		fromTable.readLock();
		
		for(Row row : resultTable.getAllRows()){
			fromTable.updateRow(row, pairs);
		}
		
		fromTable.readUnlock();
		JsonObject result = Json.createObjectBuilder()
				.add("Result", "Row(s) Updated")
				.build();
		output.output = result.toString();
		DB.addRowsToCache(toCache);
		
	}
	
	private Table trimColumns(Table table, ColumnID[] columns) throws DataTypeMismatchException{
		if(columns[0].getColumnName().equals("*")){
			return table;
		}

		ColumnDescription[] actual = table.getColumnDescription();
		ArrayList<ColumnDescription> resultCols = new ArrayList<ColumnDescription>();
		boolean foundAlready = false;
		for(ColumnID desired: columns){
			foundAlready = false;
			
			for(ColumnDescription column : actual){
				if(column.getColumnName().equals(desired.getColumnName())){
					if(desired.getTableName() == null){
						if(foundAlready){
							throw new IllegalArgumentException("Please specify names of tables when selecting columns from multiple tables");
						}
						foundAlready = true;
						resultCols.add(column);
					}
					else if(column.getTableName().equals(desired.getTableName())){
						resultCols.add(column);
					}
				}
			}
		}
		
		ColumnDescription[] forTable = new ColumnDescription[resultCols.size()];
		for(int i=0; i<forTable.length; i++){
			forTable[i] = resultCols.get(i);
		}
		Table result = new Table(forTable, table.getName() + " result");
		
		RowLinkedList.Node head = table.getHead();
		RowLinkedList.Node current = table.getHead().next;
		Row toAdd;
		int[] colNums = new int[forTable.length];
		for (int i=0; i<forTable.length; i++){
			colNums[i] = table.getIndex(forTable[i].getColumnName());
		}
		while(current != head){
			toAdd = new Row(forTable.length, null);
			for (int i=0; i<colNums.length; i++){
				toAdd.replace(i, current.row().getObjectAt(colNums[i]));// get the object at the row number
			}														  // corresponding to the original table
			
			result.addRow(toAdd);
			
			current = current.next;
		}
				
		return result;
	}
	
	private boolean tableWasUsed(String[] usedTables, String table){
		for (int i=0; i<usedTables.length; i++){
			if (table.equals(usedTables[i].trim())){
				return true;
			}
		}
		return false;
	}
	
	private void hasTableAndColumns(String table, ColumnID[] columns){
		hasTable(table);
		if(columns[0].getColumnName().trim().equals("*") && columns.length > 1){
			throw new IllegalArgumentException("Error: When inputting \"*\", do not input any other column names");
		}
		for(ColumnID column : columns){//check if one of the tables has each column
			if(!column.getColumnName().equals("*") && !DB.tableHasColumn(table, column.getColumnName()) && column.getTableName().equals(table)){
				throw new IllegalArgumentException("Column: \"" + column.getColumnName() + "\" not found in DataBase");
			}
		}
		
	}
	private void hasTable(String table){
		if (!DB.hasTable(table)){//check if database has table
			throw new IllegalArgumentException("Error: Table \"" + table + "\" not found in DataBase");
		}
	}
	
	private Table makeDistinct(Table table, ColumnID col) throws DataTypeMismatchException{
		int colNumber = table.getIndex(col.getColumnName());
		Table result = new Table(table.getColumnDescription(), table.getName());
		HashSet<Object> set = new HashSet<Object>();
		
		RowLinkedList.Node head = table.getHead();
		RowLinkedList.Node current = head.next;
		Row placeholder = null;
		while(current != head){
			placeholder = current.row();
			placeholder.readLock();
			
			if(!set.contains(placeholder.getObjectAt(colNumber))){
				set.add(placeholder.getObjectAt(colNumber));
				
				placeholder.readUnlock();
				placeholder.writeLock();
				
				result.addRow(placeholder.getCopy());
				
				placeholder.writeUnlock();
				
			} else{
				
				placeholder.readUnlock();
			}
			current = current.next;
		}
		return result;
	}
	
	private Row getMax(Table table, ColumnID col){
		
		if(table.getAllRows().size() == 0){
			return null;
		}
		
		RowLinkedList.Node head = table.getHead();
		RowLinkedList.Node current = head.next;
		Row placeholder = null;
		int colNumber = table.getIndex(col.getColumnName());
		Row maxRow = null;
	
		switch(table.getColumnDescription()[colNumber].getColumnType().toString()){
		
			case "INT":
				
				int maxInt = Integer.MIN_VALUE;
				 
				 current = head.next;
				
				while(current != head){
					placeholder = current.row();
					placeholder.readLock();
					
					if((Integer)placeholder.getObjectAt(colNumber) > maxInt){
						maxInt = (Integer) placeholder.getObjectAt(colNumber);
						maxRow = placeholder;
					}
					
					placeholder.readUnlock();
					
					current = current.next;
				}
				break;
				
			case "VARCHAR":
				maxRow = null;
				
				
				current = head.next;
				
				String maxString = (String) current.row().getObjectAt(colNumber);
				
				
				
				while(current != head){
					placeholder = current.row();
					placeholder.readLock();
					
					if( ((String)placeholder.getObjectAt(colNumber)).compareTo(maxString) > 0){
						maxString = (String) placeholder.getObjectAt(colNumber);
						maxRow = placeholder;
					}
					
					placeholder.readUnlock();
					
					current = current.next;
				}
				break;
			
			case "BOOLEAN":
				
				current = head.next;
				placeholder = current.row();
				placeholder.readLock();
				
				if(current.row().getObjectAt(colNumber).equals(true)){
					return current.row();
				}
				else{
					maxRow = current.row();
				}
				
				placeholder.readUnlock();
				
				current = current.next;
				while (current != head){//as soon as we find a true, return it
					placeholder = current.row();
					placeholder.readLock();
					
					if(placeholder.getObjectAt(colNumber).equals(true)){
						placeholder.readUnlock();
						return placeholder;
					}
					
					placeholder.readUnlock();
					
					current = current.next;
				}
				break;
				
	
			case "DECIMAL":
				
				Float maxFloat = Float.MIN_VALUE;
				 
				 current = head.next;
				
				while(current != head){
					placeholder = current.row();
					placeholder.readLock();
					
					if((Float)placeholder.getObjectAt(colNumber) > maxFloat){
						maxFloat = (Float) placeholder.getObjectAt(colNumber);
						maxRow = placeholder;
					}
					
					placeholder.readUnlock();
					
					current = current.next;
				}
				break;
				
				
		}
		return maxRow;
		
	}
	
private Row getMin(Table table, ColumnID col){
		
		if(table.getAllRows().size() == 0){
			return null;
		}
		
		RowLinkedList.Node head = table.getHead();
		RowLinkedList.Node current = head.next;
		Row placeholder = null;
		
		int colNumber = table.getIndex(col.getColumnName());
		Row minRow = null;
		switch(table.getColumnDescription()[colNumber].getColumnType().toString()){
		
			case "INT":
				
				int minInt = Integer.MAX_VALUE;
				 
				 current = head.next;
				
				while(current != head){
					placeholder = current.row();
					placeholder.readLock();
					
					if((Integer)placeholder.getObjectAt(colNumber) < minInt){
						minInt = (Integer) placeholder.getObjectAt(colNumber);
						minRow = placeholder;
					}
					
					placeholder.readUnlock();
					
					current = current.next;
				}
				break;
				
			case "VARCHAR":
				minRow = null;
				
				
				current = head.next;
				
				String minString = (String) current.row().getObjectAt(colNumber);
				
				
				
				while(current != head){
					placeholder = current.row();
					placeholder.readLock();
					
					if( ((String)placeholder.getObjectAt(colNumber)).compareTo(minString) < 0){
						minString = (String) placeholder.getObjectAt(colNumber);
						minRow = placeholder;
					}
					
					placeholder.readUnlock();
					
					current = current.next;
				}
				break;
			
			case "BOOLEAN":
				
				current = head.next;
				placeholder = current.row();
				placeholder.readLock();
				
				if(placeholder.getObjectAt(colNumber).equals(false)){
					return placeholder;
				}
				else{
					minRow = placeholder;
				}
				
				placeholder.readUnlock();
				
				current = current.next;
				while (current != head){//as soon as we find a true, return it
					placeholder = current.row();
					placeholder.readLock();
					
					if(placeholder.getObjectAt(colNumber).equals(false)){
						placeholder.readUnlock();
						return placeholder;
					}
					
					placeholder.readUnlock();
					
					current = current.next;
				}
				break;
				
	
			case "DECIMAL":
				
				Float minFloat = Float.MAX_VALUE;
				 
				 current = head.next;
				
				while(current != head){
					placeholder = current.row();
					placeholder.readLock();
					
					if((Float)placeholder.getObjectAt(colNumber) < minFloat){
						minFloat = (Float) placeholder.getObjectAt(colNumber);
						minRow = placeholder;
					}
					
					placeholder.readUnlock();
					
					current = current.next;
				}
				break;
				
				
		}
		return minRow;
		
	}

	private Double getSum(Table table, ColumnID col){
		int colNumber = table.getIndex(col.getColumnName());
		String type = table.getColumnDescription()[colNumber].getColumnType().toString();
		if(table.getAllRows().size() == 0 || type.equals("VARCHAR") || type.equals("BOOLEAN")){
			return 0.00;
		}
		
		Double result = 0.00;

		RowLinkedList.Node head = table.getHead();
		RowLinkedList.Node current = head.next;
		Row placeholder = null;
		while(current != head){
			placeholder = current.row();
			placeholder.readLock();
			
			result += ((Number)placeholder.getObjectAt(colNumber)).doubleValue();
			
			placeholder.readUnlock();
			
			current = current.next;
		}
		
		return result;
		
	}
	
	private Double getAvg(Table table, ColumnID col){
		Double result = getSum(table, col);
		
		return result/table.getRowCount();
		
	}
	
}

