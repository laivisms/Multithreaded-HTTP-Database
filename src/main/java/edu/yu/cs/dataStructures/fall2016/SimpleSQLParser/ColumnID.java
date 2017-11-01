package edu.yu.cs.dataStructures.fall2016.SimpleSQLParser;

/**
 * identifies a a column using the column name and table name
 * @author diament@yu.edu
 *
 */
public class ColumnID
{
    private String columnName;
    private String tableName;
    
    public ColumnID(String columnName, String tableName)
    {
	this.columnName = columnName;
	this.tableName = tableName;
    }   
    public String getColumnName()
    {
	return this.columnName;
    }
    public String getTableName()
    {
	return this.tableName;
    }
    /**
     * @return the table name, followed by a period, followed by the column name
     */
    public String toString()
    {
	if(this.tableName != null)
	{
	    return this.tableName + "." + this.columnName;
	}
	return this.columnName;
    }
}