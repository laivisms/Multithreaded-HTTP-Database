package edu.os.database.DataBase;


public class InvalidLengthException extends Exception {

	private static final long serialVersionUID = 1L;
	
	public InvalidLengthException(String type, String column){
		super("The " + type + " entered for column " + column + " is too long.");
	}
}

