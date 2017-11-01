package edu.os.database.DataBase;

/*
 * thrown if row input doesnt match required column type
 */
public class DataTypeMismatchException extends Exception {

	private static final long serialVersionUID = 1L;
	
	public DataTypeMismatchException(String column, String required){
		super("Wrong input type for column: \"" + column + "\". Input of type " + required + " is required.");
	}

}
