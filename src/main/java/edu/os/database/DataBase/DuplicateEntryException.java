package edu.os.database.DataBase;


public class DuplicateEntryException extends Exception {

	private static final long serialVersionUID = 1L;
	
	public DuplicateEntryException(String column){
		super("Column " + column + " cannot have duplicate entries");
	}
}

