package edu.os.database.DataBase;

/*
 * Used for when a parameter inputted into a row in a table is null, but the table specifies 
 * that all entries in that column must not be null
 */
public class NullEntryException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public NullEntryException(){
		super("An entry in a column must contain a non-null value");
	}
	
	public NullEntryException(String colName){
		super("Entry in column" + colName + "must not be null");
	}
}

