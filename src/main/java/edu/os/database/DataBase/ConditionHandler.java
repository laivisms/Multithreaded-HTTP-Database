package edu.os.database.DataBase;


//import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
//import java.util.List;
//import java.util.ListIterator;
//import java.util.NoSuchElementException;


import edu.yu.cs.dataStructures.fall2016.SimpleSQLParser.ColumnDescription;
import edu.yu.cs.dataStructures.fall2016.SimpleSQLParser.ColumnID;
import edu.yu.cs.dataStructures.fall2016.SimpleSQLParser.Condition;
/**
 * conditionHandler class, solves all of the where conditions. It works basically like this:
 * The where condition is a tree, so the parseCondition function recursively returns Tables that have been enacted upon by the where. For
 * example, if there was a where that stated table1.col1  = 5, it would return a new table, containing only those rows which satisfy 
 * that condition. The table is returned up the tree to be enacted upon by a AND or OR, so that only copies of tables get returned to
 * be enacted upon by an AND or OR. the result is one table that has rows from different tables concatenated, etc, according to the need of the
 * where. if there were any tables that were not joined to the table at the end of the where, the QueryHandler cartesianProducts the unused tables
 * onto the result table.
 *  
 * @author SL
 *
 */
public class ConditionHandler {
	
	public ConditionHandler() {

	}

	public static Table parseCondition(Condition where, HashMap<String, Table> tables, String[] tableNames, ArrayList<Row> toCache)
			throws DataTypeMismatchException {
		Table result = null;
		Object left = where.getLeftOperand(); // on every iteration of the
												// where, replace the table in
												// the map with the new table
		Object right = where.getRightOperand();
		String op = where.getOperator().toString();

		if (left instanceof Condition) {
			left = parseCondition((Condition) left, tables, tableNames, toCache);// left is now a
															// new, composite table
		}
		if (right instanceof Condition) {
			right = parseCondition((Condition) right, tables, tableNames, toCache);// right is now a
																//new, composite table
		}

		if (left instanceof Table && right instanceof Table) { // only for AND
			Table leftTable = (Table) left;												// and OR
			Table rightTable = (Table) right;
			switch (op) {

			case "=":
				throw new IllegalArgumentException("Error in conditionHandler, Table Table =");


			case "<>":
				throw new IllegalArgumentException("Error in conditionHandler, Table Table <>");


			case "<":
				throw new IllegalArgumentException("Error in conditionHandler, Table Table <");


			case "<=":
				throw new IllegalArgumentException("Error in conditionHandler, Table Table <=");


			case ">":
				throw new IllegalArgumentException("Error in conditionHandler, Table Table >");


			case ">=":
				throw new IllegalArgumentException("Error in conditionHandler, Table Table >=");


			case "AND":
			case "OR":
				return andOrTables(leftTable, rightTable, op);
			}
		}
		

		if (right instanceof ColumnID && left instanceof ColumnID) {// table1.column
																	// =
																	// table2.column
																	// query

			ColumnID leftCol = (ColumnID) left;
			ColumnID rightCol = (ColumnID) right;

			Table leftTable = tables.get(leftCol.getTableName());
			Table rightTable = tables.get(rightCol.getTableName());

			switch (op) {
			case "=":
				result = evalEqualsColumn(leftTable, leftCol, rightTable, rightCol, toCache);
				break;

			case "<>":
			case "<":
			case "<=":
			case ">":
			case ">=":
				result = evalKarrotColumn(leftTable, leftCol, rightTable, rightCol, op, toCache);
				break;
				
			case "AND":
				throw new IllegalArgumentException("Error in ConditionHandler, CASE ColumnID AND");

			case "OR":
				throw new IllegalArgumentException("Error in ConditionHandler, CASE ColumnID OR");
			}
			
			
			return result;
		}

		if ((right instanceof ColumnID && left instanceof String)
				|| (right instanceof String && left instanceof ColumnID)) {
			String value;
			ColumnID column;
			boolean onLeft = true;

			if (left instanceof String) {// left is string, right is columnID
				value = (String) left;
				column = (ColumnID) right;
				onLeft = false;
			}

			else if (right instanceof String) {// opposite
				value = (String) right;
				column = (ColumnID) left;
			}

			else {// error
				throw new IllegalArgumentException("Should be ColumnID and String, was not");
			}
			String tableName = column.getTableName();
			if(tableName == null){
				tableName = getTableName(column, tables, tableNames);
			}
			Table table = tables.get(tableName);

			switch (op) {
			case "=":
				result = evalEqualsString(table, column, value, toCache);
				break;
			case "<>":
				result = evalKarrotString(table, column, value, op, toCache);
				break;
			case "<":
				if (onLeft) {// the column was on the left side, operator is
								// as intended
					result = evalKarrotString(table, column, value, op, toCache);
					break;
				}
				result = evalKarrotString(table, column, value, ">", toCache); // value/table order was backwords
				break;
			case "<=":
				if (onLeft) {
					result = evalKarrotString(table, column, value, op, toCache);
					break;
				}
				result = evalKarrotString(table, column, value, ">=", toCache);
				break;

			case ">":
				if (onLeft) {
					result = evalKarrotString(table, column, value, op, toCache);
					break;
				}
				result = evalKarrotString(table, column, value, "<", toCache);
				break;

			case ">=":
				if (onLeft) {
					result = evalKarrotString(table, column, value, op, toCache);
					break;
				}
				result = evalKarrotString(table, column, value, "<=", toCache);
				break;

			case "AND":
				throw new IllegalArgumentException("Error in ConditionHandler, CASE String AND");

			case "OR":
				throw new IllegalArgumentException("Error in ConditionHandler, CASE String OR");
			}
			
			
			return result;
		}
		
		return null;

	}
	/*
	 * helper class, purely for andTables
	 */
	static class Pair{
		int begin;
		int end;
		Pair leftTable;
		Pair rightTable;
		
		Pair(Pair leftP, Pair rightP){
			leftTable = leftP;
			rightTable = rightP;
		}
		
		Pair(int left, int right){
			begin = left;
			end = right;
		}
	}
	
	
	private static Table andOrTables(Table left, Table right, String op) throws DataTypeMismatchException{
		if(!containsSomeTables(left, right)){
			return cartesianProduct(left, right);
		}
		else if(sameColumns(left, right) && op.equals("OR")){//if the tables have the same columns which may or may not be in a different order,
			ArrayList<Pair> pairs = findColumnPairs(left.getColumnDescription(), right.getColumnDescription());//find where the columns are the same
			ArrayList<Row> toAdd = new ArrayList<Row>();
			
			RowLinkedList.Node rightHead = right.getHead();
			RowLinkedList.Node rightNode = rightHead.next;
			RowLinkedList.Node leftHead = left.getHead();
			RowLinkedList.Node leftNode;
			Row rightRow = null;
			Row leftRow = null;
			Row temp = null;
			boolean isInTable;
			while(rightNode != rightHead){//for every node in the right table,
				rightRow = rightNode.row();
				
				temp = reArrangeColumns(rightRow, pairs);// rearrange the order of the columns so they match the left columns
				isInTable = false;
				leftNode = leftHead.next;
				
				while(leftNode != leftHead){
					leftRow = leftNode.row();
					if(leftRow.equals(temp)){//if the row is already in the table, not adding it
						isInTable = true;
						leftNode = leftHead.prev;//end the loop, not adding this row
					}
					leftNode = leftNode.next;
				}
				if(!isInTable){ //if the row was not found in the table, add to results
					toAdd.add(temp);
				}
				
				rightNode = rightNode.next;
			}
			
			for(Row row : toAdd){
				left.addRow(row);
			}
			return left;
			
		}
		ArrayList<Pair> pairs = findColumnPairs(left.getColumnDescription(), right.getColumnDescription());
		ColumnDescription[] newTableColumns = combineColumnDescriptions(left.getColumnDescription(), right.getColumnDescription(), pairs);
		Table result = new Table(newTableColumns, left.getName() + ", " + right.getName());
		RowLinkedList.Node leftHead = left.getHead();
		RowLinkedList.Node leftNode = leftHead.next;
		RowLinkedList.Node rightHead = right.getHead();
		RowLinkedList.Node rightNode;
		Row leftRow = null;
		Row rightRow = null;
		
		while(leftNode != leftHead){
			rightNode = rightHead.next;
			leftRow = leftNode.row();
			leftRow.readLock();
			
			while(rightNode != rightHead){
				rightRow = rightNode.row();
				rightRow.readLock();
				switch(op){
				
				case"OR":
					leftRow.readUnlock();
					leftRow.writeLock();
					rightRow.readUnlock();
					rightRow.writeLock();
					
					
					result.addRow(combineRows(leftRow, rightRow, pairs));//adds the rows by removing the right row's similar columns then concat
					result.addRow(replaceLeftObjectsWithRightObjects(leftRow, rightRow, pairs));//adds the rows by replacing the matching columns in the left row with the corresponding ones in the right, then concat the remaining right onto the left
					
					rightRow.writeUnlock();
					leftRow.writeUnlock();
					leftRow.readLock();
					break;
				
					
				case"AND":
					if(containsSubset(leftRow, rightRow, pairs)){
						leftRow.readUnlock();
						leftRow.writeLock();
						
						
						result.addRow(combineRows(leftRow, rightRow, pairs));
						
						
						leftRow.writeUnlock();
						leftRow.readLock();
						rightRow.readUnlock();
						rightNode = rightHead.prev;//end the loop
					}
					else{
						rightRow.readUnlock();
					}
					break;
					
				}
				
				rightNode = rightNode.next;
			}
			
			
			rightNode = rightHead.next;
			
			leftRow.readUnlock();
			
			leftNode = leftNode.next;
		}
		return result;
	}


	private static Table evalEqualsColumn(Table left, ColumnID leftCol, Table right, ColumnID rightCol, ArrayList<Row> toCache)
			throws DataTypeMismatchException {
		
		Table result = getCombinedTableClone(left, right);
		joinOn(left, leftCol, right, rightCol, result, toCache);
		return result;
	}
	
	private static Table evalKarrotColumn(Table left, ColumnID leftCol, Table right, ColumnID rightCol, String op, ArrayList<Row> toCache) throws DataTypeMismatchException{
		RowLinkedList rows = (RowLinkedList) right.getAllRows();
		int colIndex = right.getIndex(rightCol.getColumnName());
		ArrayList<Row> totalResults = new ArrayList<Row>();
		
		right.readLock();
		
		ArrayList<Row> resultSet;
		String toCompare;
		Row rowCopy;
		RowLinkedList.Node current = rows.getHead().next;
		Row placeholder = null;
		while(current != rows.getHead()){ // iterate through all the rows
			placeholder = current.row();
			placeholder.writeLock();
			
			//add the row to be cached
			rowCopy = placeholder.getCopy();
			current.cacheRow(rowCopy);
			toCache.add(rowCopy);
			
			placeholder.writeUnlock();
			
			toCompare = placeholder.getObjectAt(colIndex) + "";
			
			resultSet = evalKarrotStringArray(left, leftCol, toCompare, op, toCache);//evaluate each row at colIndex, depending on operator
			
			totalResults.addAll(cartesianProduct(resultSet, rowCopy));// then take the cartesian product of the results
			current = current.next;
		}
		
		right.readUnlock();
		
		Table result = getCombinedTableClone(left, right);
		fillTable(result, totalResults);
		return result;
	}
	
	
	
	private static Table evalEqualsString(Table table, ColumnID column, String toMatch, ArrayList<Row> toCache)
			throws DataTypeMismatchException {
		ColumnDescription[] rightColumns = table.getColumnDescription();
		Table result = new Table(rightColumns, table.getName());
	
		table.readLock();
	
		ArrayList<RowLinkedList.Node> matches = table.getRowsWhere(column.getColumnName(), toMatch);
	
		
	
		int matchesSize = matches.size();
		Row match = null;
		for (int i = 0; i < matchesSize; i++) {
			match = matches.get(i).row();
			
			match.writeLock();
			
			//add row to list which will be cached
			Row toAdd = match.getCopy();
			matches.get(i).cacheRow(toAdd);
			toCache.add(toAdd);
	
			match.writeUnlock();
	
			result.addRow(toAdd);
		}
		
		table.readUnlock();
		return result;
	}

	private static Table evalKarrotString(Table table, ColumnID columnID, String toMatch, String op, ArrayList<Row> toCache) throws DataTypeMismatchException{
		ColumnDescription[] columns = table.getColumnDescription();
		Table result = new Table(columns, table.getName());
		ArrayList<Row> resultSet = evalKarrotStringArray(table, columnID, toMatch, op, toCache);
		fillTable(result, resultSet);
		return result;
	}

	private static ArrayList<Row> evalKarrotStringArray(Table table, ColumnID columnID, String toMatch, String op, ArrayList<Row> toCache)
			throws DataTypeMismatchException {
	
		ColumnDescription[] columns = table.getColumnDescription();
		ArrayList<Row> result = new ArrayList<Row>();
		int column = table.getIndex(columnID.getColumnName());
		Object value = table.convertValue(columns[column].getColumnType(), toMatch);
	
		table.readLock();
		
		RowLinkedList rows = (RowLinkedList) table.getAllRows();
		RowLinkedList.Node current = rows.getHead().next;
		Row row = null;
		while (current != rows.getHead()) {
			row = current.row();
			
			row.readLock();
			
			switch (op){
				case "<>":
					try {
						if (((Comparable) row.getObjectAt(column)).compareTo(value) != 0) {
							
							row.readUnlock();
							row.writeLock();
							
							Row rowCopy = row.getCopy();
							result.add(rowCopy);
							
							//add rowCopy to list which will be cached
							current.cacheRow(rowCopy);
							toCache.add(rowCopy);
							
							row.writeUnlock();//downgrade to read lock
						
						} else{ row.readUnlock();}
					} catch (ClassCastException e) {
						System.out.println("problem casting in evalLessThanString");
						row.readUnlock();
					}
					break;
				case "<":
					
					try {
						if (((Comparable) row.getObjectAt(column)).compareTo(value) < 0) {
							
							row.readUnlock();
							row.writeLock();
							
							Row rowCopy = row.getCopy();
							result.add(rowCopy);
							
							//add rowCopy to list which will be cached
							current.cacheRow(rowCopy);
							toCache.add(rowCopy);
							
							row.writeUnlock();//downgrade to read lock
							
							row.writeUnlock();
						} else{ row.readUnlock();}
					} catch (ClassCastException e) {
						System.out.println("problem casting in evalLessThanString");
						row.readUnlock();
					}
					break;
				case "<=":
					try {
						if (((Comparable) row.getObjectAt(column)).compareTo(value) <= 0) {
							
							row.readUnlock();
							row.writeLock();
							
							Row rowCopy = row.getCopy();
							result.add(rowCopy);
							
							//add rowCopy to list which will be cached
							current.cacheRow(rowCopy);
							toCache.add(rowCopy);
							
							row.writeUnlock();//downgrade to read lock
							
						}else{ row.readUnlock();}
					} catch (ClassCastException e) {
						System.out.println("problem casting in evalLessThanString");
						row.readUnlock();
					}
					break;
				case ">":
					try {
						if (((Comparable) row.getObjectAt(column)).compareTo(value) > 0) {
							
							
							row.readUnlock();
							row.writeLock();
							
							Row rowCopy = row.getCopy();
							result.add(rowCopy);
							
							//add rowCopy to list which will be cached
							current.cacheRow(rowCopy);
							toCache.add(rowCopy);
							
							row.writeUnlock();//downgrade to read lock
							
						}else{ row.readUnlock();}
					} catch (ClassCastException e) {
						System.out.println("problem casting in evalLessThanString");
						row.readUnlock();
					}
					break;
				case ">=":
					try {
						if (((Comparable) row.getObjectAt(column)).compareTo(value) >= 0) {
							
							
							row.readUnlock();
							row.writeLock();
							
							Row rowCopy = row.getCopy();
							result.add(rowCopy);
							
							//add rowCopy to list which will be cached
							current.cacheRow(rowCopy);
							toCache.add(rowCopy);
							
							row.writeUnlock();//downgrade to read lock
							
						}else{ row.readUnlock();}
					} catch (ClassCastException e) {
						System.out.println("problem casting in evalLessThanString");
						row.readUnlock();
					}
					break;
			}
			
			current = current.next;
		}
	
		table.readUnlock();
		
		return result;
	}

	/*
	 * a test to see whether columns are ambiguous, ie, there is more than one table with the specific column
	 * name, and the table name was not specified
	 */
	private static String getTableName(ColumnID column, HashMap<String, Table> tables, String[] tableNames) {
		String name = null;
		for(String table : tableNames){
			if(tables.get(table).getIndex(column.getColumnName()) != -1){
				if(name != null){
					throw new IllegalArgumentException("Please Specify tables of columns when dealing with tables containing the same column names");
				}
				name = table;
			}
		}
		return name;
		
	}

	private static void fillTable(Table table, ArrayList<Row> rows) throws DataTypeMismatchException{
		for(Row row : rows){
			table.addRow(row);
		}
	}
	
	private static ArrayList<Row> cartesianProduct(ArrayList<Row> rows, Row right){
		ArrayList<Row> results = new ArrayList<Row>();
		for(Row row : rows){
			results.add(row.concat(right));
		}
		return results;
	}
	
	private static Table getCombinedTableClone(Table left, Table right){
		ColumnDescription[] leftColumns = left.getColumnDescription();
		ColumnDescription[] rightColumns = right.getColumnDescription();

		ColumnDescription[] allCol = concatArrays(leftColumns, rightColumns);
		return new Table(allCol, left.getName() + ", " + right.getName());
	}

	private static ColumnDescription[] concatArrays(ColumnDescription[] left, ColumnDescription[] right) {
		ColumnDescription[] result = new ColumnDescription[left.length + right.length];
		int counter = 0;
		for (ColumnDescription column : left) {
			result[counter] = column;
			counter++;
		}
		for (ColumnDescription column : right) {
			result[counter] = column;
			counter++;
		}
		return result;
	}

	/*
	 * appends the right table to every row on the left table, on the condition
	 * that left.ColumnID.getColumnName() == right.ColumnID.getColumnName().
	 * inputs the resulting rows into the output
	 */
	private static void joinOn(Table left, ColumnID leftCol, Table right, ColumnID rightCol, Table output, ArrayList<Row> toCache)
			throws DataTypeMismatchException {
		left.readLock();
		right.readLock();
		int leftIndex = left.getIndex(leftCol.getColumnName());
		
		
		RowLinkedList.Node node = left.getHead().next;
		Row placeholder = null;
		while (node != left.getHead()) {
			placeholder = node.row();
			placeholder.readLock();

			Object toMatch = placeholder.getObjectAt(leftIndex);// find the object to match to
															
			ArrayList<RowLinkedList.Node> matches = right.getRowsWhere(rightCol.getColumnName(), toMatch); // get
																							// all
																							// the
																							// matches
			int matchesSize = matches.size();
			for (int i = 0; i < matchesSize; i++) { // iterate over the matches,
													// concatenating them to the
													// row.
				
				Row current = matches.get(i).row();
				current.writeLock();
				placeholder.readUnlock();
				placeholder.writeLock();
				
				//make a copy for the cache, add it to the list to return to cache
				Row copy = current.getCopy();
				matches.get(i).cacheRow(copy);
				toCache.add(copy);

				Row toAdd = placeholder.concat(current);

				placeholder.writeUnlock();
				placeholder.readLock();
				current.writeUnlock();
				

				output.addRow(toAdd);
			}

			placeholder.readUnlock();
			
			
			node = node.next;

		}

		right.readUnlock();
		left.readUnlock();
	}
	
	private static boolean containsSomeTables(Table table1, Table table2){
		String[] t1 = table1.getName().split(",");
		String[] t2 = table2.getName().split(",");
		int t1Length = t1.length;
		int t2Length = t2.length;
		String t1String = null;
		for (int i=0; i<t1Length; i++){
			t1String = t1[i].trim();
			for(int j=0; j<t2Length; j++){
				if(t1String.equals(t2[j].trim())){
					return true;
				}
			}
		}
		return false;
	}
	
	private static boolean containsSubset(Row r1, Row r2, ArrayList<Pair> pairs){
		Pair left, right;
		Object leftObj, rightObj;
		for (Pair pair : pairs){
			left = pair.leftTable;
			right = pair.rightTable;
			
			for (int i=left.begin, j=right.begin; i<left.end; i++, j++){//for each column which came from the same table
				leftObj = r1.getObjectAt(i);
				rightObj = r2.getObjectAt(j);
				if((leftObj == null && rightObj != null) || 
					(leftObj != null && rightObj == null) ||
					((leftObj != null && rightObj != null) &&
						!leftObj.equals(rightObj))){// if they are not equal
					return false; //these two rows are not a match for eachother, return false
				}
			}
		}
		return true;
	}
	
	/**
	 * returns pairs of all begin indexes and end indexes in the left and right tables where the columns
	 * in the left table are the same as the columns from the right table(they are the same name and come, originally,
	 * from the same table. since these are composite tables, they may have the same columns, but in different 
	 * starting and ending positions
	 *  
	 * @param left
	 * @param right
	 * @return
	 */
	private static ArrayList<Pair> findColumnPairs(ColumnDescription[] left, ColumnDescription[] right){
		ArrayList<Pair>  pairs= new ArrayList<Pair>();
		Pair leftPair;
		Pair rightPair;
		int beginLeft;
		int beginRight;
		
		for(int i=0; i<left.length; i++){
			for (int j=0; j< right.length; j++){
				if (i<left.length && left[i].getColumnName().equals(right[j].getColumnName()) && 
						left[i].getTableName().equals(right[j].getTableName())){//if theyre from the same column and table
					beginLeft = i;
					beginRight = j;
					while(i< left.length && j< right.length && left[i].getColumnName().equals(right[j].getColumnName())){
						i++;
						j++;
					}
					leftPair = new Pair(beginLeft, i);
					rightPair = new Pair(beginRight, j);
					pairs.add(new Pair(leftPair, rightPair));
				}
			}
		}
		return pairs;
	}
	
	private static Row combineRows(Row left, Row right, ArrayList<Pair> pairs){
		Row rightCopy = right.getCopy();
		Pair doNotCopy = new Pair(null, null);
		for(Pair pair : pairs){
			for(int i=pair.rightTable.begin; i<pair.rightTable.end; i++){
				rightCopy.replace(i, doNotCopy);
			}
		}
		ArrayList<Object> rightDifference = new ArrayList<Object>();
		for(int i=0; i< right.getSize(); i++){
			if(rightCopy.getObjectAt(i) != doNotCopy){
				rightDifference.add(right.getObjectAt(i));
			}
		}
		Row result = new Row(left.getSize() + rightDifference.size(), left.getTableName() + right.getTableName());
		for(int i=0; i<left.getSize(); i++){
			result.add(left.getObjectAt(i));
		}
		for(Object addition : rightDifference){
			result.add(addition);
		}
		return result;
	}
	
	private static ColumnDescription[] combineColumnDescriptions(ColumnDescription[] left, ColumnDescription[] right, ArrayList<Pair> pairs){
		ArrayList<ColumnDescription> results = new ArrayList<ColumnDescription>();
		ColumnDescription[] rightCopy = right.clone();
		for(Pair pair : pairs){
			for(int i=pair.rightTable.begin; i<pair.rightTable.end; i++ ){
				rightCopy[i] = null;
			}
		}
		for(ColumnDescription column : left){
			results.add(column);
		}
		for(ColumnDescription column : rightCopy){
			if(column != null){
				results.add(column);
			}
		}
		ColumnDescription[] result = new ColumnDescription[results.size()];
		for (int i=0; i<result.length; i++){
			result[i] = results.get(i);
		}
		
		return result;
	}
	
	public static Table cartesianProduct(Table left, Table right) throws DataTypeMismatchException{
		Table result = getCombinedTableClone(left, right);
		
		RowLinkedList.Node leftHead = left.getHead();
		RowLinkedList.Node rightHead = right.getHead();
		RowLinkedList.Node leftNode = leftHead.next;
		RowLinkedList.Node rightNode;
		Row leftRow = null;
		Row rightRow = null;
		while (leftNode != leftHead){
			rightNode = rightHead.next;
			leftRow = leftNode.row();
			
			leftRow.writeLock();
			
			
			while(rightNode != rightHead){
				rightRow = rightNode.row();
				
				rightRow.writeLock();
				
				result.addRow(leftRow.concat(rightRow));
				
				rightRow.writeUnlock();
				
				rightNode = rightNode.next;
			}
			
			
			leftRow.writeUnlock();
			
			
			leftNode = leftNode.next;
		}
		
		return result;
		
	}
	
	private static boolean sameColumns(Table left, Table right){
		ColumnDescription[] leftCols = left.getColumnDescription();
		ColumnDescription[] rightCols = right.getColumnDescription();
		if(rightCols.length != leftCols.length){
			return false;
		}
		for(ColumnDescription column : leftCols){
			if(!right.hasColumn(column.getColumnName())){
				return false;
			}
		}
		for (ColumnDescription column : rightCols){
			if(!left.hasColumn(column.getColumnName())){
				return false;
			}
		}
		return true;
	}
	
	private static Row reArrangeColumns(Row row, ArrayList<Pair> pairs){
		Row temp = new Row(row.getSize(), row.getTableName());
		for(Pair pair : pairs){
			Pair right = pair.rightTable;
			Pair left = pair.leftTable;
			for(int i=left.begin, j = right.begin; i < left.end && j< right.end; i++, j++){
				temp.replace(i, row.getObjectAt(j));
			}
		}
		return temp;
	}
	
	private static Row replaceLeftObjectsWithRightObjects(Row left, Row right, ArrayList<Pair> pairs){
		Row rightCopy = right.getCopy();
		Row leftCopy = left.getCopy();
		Pair doNotCopy = new Pair(null, null);
		Pair leftPair = null;
		Pair rightPair = null;
		for(Pair pair : pairs){
			leftPair = pair.leftTable;
			rightPair = pair.rightTable;
			for(int i=leftPair.begin, j= rightPair.begin; i<leftPair.end && j<rightPair.end; i++, j++){
				leftCopy.replace(i, right.getObjectAt(j));
				rightCopy.replace(i, doNotCopy);
			}
		}
		ArrayList<Object> rightDifference = new ArrayList<Object>();
		for(int i=0; i< right.getSize(); i++){
			if(rightCopy.getObjectAt(i) != doNotCopy){
				rightDifference.add(right.getObjectAt(i));
			}
		}
		Row result = new Row(left.getSize() + rightDifference.size(), left.getTableName() + right.getTableName());
		for(int i=0; i<left.getSize(); i++){
			result.add(left.getObjectAt(i));
		}
		for(Object addition : rightDifference){
			result.add(addition);
		}
		return result;
		
		
	}
	

}
