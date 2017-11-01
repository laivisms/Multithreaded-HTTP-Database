package edu.os.database.DataBase;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
//import java.sql.Date;
import java.sql.Timestamp;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
//import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
//import javax.swing.text.Position;

import org.apache.commons.collections4.queue.CircularFifoQueue;

import edu.yu.cs.dataStructures.fall2016.SimpleSQLParser.ColumnDescription;
import edu.yu.cs.dataStructures.fall2016.SimpleSQLParser.CreateTableQuery;
import edu.yu.cs.dataStructures.fall2016.SimpleSQLParser.InsertQuery;
import edu.yu.cs.dataStructures.fall2016.SimpleSQLParser.SQLParser;
//import edu.yu.cs.dataStructures.fall2016.SimpleSQLParser.SQLQuery;
import net.sf.jsqlparser.JSQLParserException;

public class DataBase {
	private ArrayList<Table> tables = new ArrayList<Table>();
	private ArrayList<String> tableNames = new ArrayList<String>();
	private ConcurrentHashMap<String, Table> tableMap = new ConcurrentHashMap<String, Table>();
	private ReentrantReadWriteLock DBLock = new ReentrantReadWriteLock(true);
	private ReentrantReadWriteLock mapLock = new ReentrantReadWriteLock(true);
	private CircularFifoQueue<Row> cache = new CircularFifoQueue<Row>(10);
	private int editCount;
	private final int MAX_EDITS = 5;
	private String OUTPUT_FILE = "backups/output";//will append .txt
	private static final String DATE_FORMAT = "yyyy:MM:dd:HH:mm:ss:SS";
	private SQLParser parser = new SQLParser();
	
	public DataBase(){
		
	}
	
	public void readLock(){
		DBLock.readLock().lock();
	}
	
	public void readUnlock(){
		DBLock.readLock().unlock();
	}
	
	public void writeLock(){
		DBLock.writeLock().lock();
	}
	
	public void writeUnlock(){
		DBLock.writeLock().unlock();
	}
	
	public boolean hasTable(String table){
		return tableNames.contains(table);
	}
	
	public boolean tableHasColumn(String table, String column){
		return tableMap.get(table).hasColumn(column);
	}
	
	 java.util.Date retreiveTables() throws NullEntryException, DataTypeMismatchException, DuplicateEntryException, InvalidLengthException, IOException{
		File currentFile = new File(OUTPUT_FILE + ".txt");
		java.util.Date date = null;
		if(currentFile.exists()){
			SimpleDateFormat form = new SimpleDateFormat(DATE_FORMAT);
			ParsePosition p = new ParsePosition(0);
			BufferedReader buffR = null;
			try{
				buffR = new BufferedReader(new FileReader(OUTPUT_FILE + ".txt"));
				String currentLine = buffR.readLine();
				
				date = form.parse(currentLine, p);
				Table currentTable = null;
				while ((currentLine = buffR.readLine()) != null) {//for every table
					currentLine = buffR.readLine();
					CreateTableQuery query =(CreateTableQuery) parser.parse(currentLine);
					createTable(query);
					currentTable = tableMap.get(query.getTableName());
					StringBuffer stringRow = new StringBuffer();
					stringRow.append("insert into " + query.getTableName() + " ( ");
					ColumnDescription[] columns = query.getColumnDescriptions();
					
					for(int i=0; i<columns.length; i++){
						if(i == columns.length-1){
							stringRow.append(columns[i].getColumnName() + ") values (");
						}
						else{
							stringRow.append(columns[i].getColumnName() + ", ");
						}
					}
					String toQuery = null;
					currentLine = buffR.readLine();
					currentLine = buffR.readLine();
					while(!currentLine.trim().equals("}")){//for every row
						int end = currentLine.lastIndexOf("|");
						toQuery = stringRow.toString() + currentLine.substring(0, end) + ");";
						currentTable.addRow((InsertQuery)parser.parse(toQuery));
						currentLine = buffR.readLine();
					}
					
				}
				buffR.close();
			}catch(IOException | JSQLParserException e){
				e.printStackTrace();
				buffR.close();
			}
		}
		return date;
	}
	
	
	
	public void createTable(CreateTableQuery template){
		Table newTable = new Table(template);
		tables.add(newTable);
		tableNames.add(template.getTableName());
		mapLock.writeLock().lock();
		
		tableMap.put(newTable.getName(), newTable);
		
		mapLock.writeLock().unlock();
	}
	
	public Table getTable(String name){
		
		mapLock.readLock().lock();
		
		Table result = tableMap.get(name);
		
		mapLock.readLock().unlock();
		
		return result;
	}
	
	void persistDB(){
		
	
		DBLock.writeLock();
		for(Table table : tables){
			table.writeLock();
		}
		
		int fileCount = 0;
		File oldFile = new File(OUTPUT_FILE + ".txt");
		File currentFile = new File(OUTPUT_FILE + ".txt");
		while(currentFile.exists()){
			currentFile = new File(OUTPUT_FILE + fileCount + ".txt");
			fileCount++;
		}
		if(fileCount>0){
			oldFile.renameTo(currentFile);
		}
		PrintWriter output = null;
		try{
			File fileToUse = new File(OUTPUT_FILE + ".txt");
			fileToUse.getParentFile().mkdirs();
			fileToUse.createNewFile();
	
			FileWriter fileW = new FileWriter(OUTPUT_FILE + ".txt", true);
			BufferedWriter buffW = new BufferedWriter(fileW);
			output = new PrintWriter(buffW);
			output.println(new SimpleDateFormat(DATE_FORMAT).format(new Timestamp(System.currentTimeMillis())));
			for(Table table : tables){
				output.println("{");
				output.println(table.getOriginalQuery());
				output.println(table.toString());
				output.println("}");
			}
		} catch(Exception e){
			if (output != null){
				output.close();
			}
			System.out.println("issue with persisting detected");
		}
		output.close();
		
		
		
		
		for(Table table : tables){
			table.writeUnlock();
		}
		
		
		
		DBLock.writeLock();
	}

	
	void addEdits(int edits){
		this.editCount += edits;
		if(editCount > MAX_EDITS){
			persistDB();
			editCount = 0;
		}
	}
	
	public String getAllTableNames(){
		if(tableNames.size() == 0){
			return "No Tables";
		}
		StringBuilder result = new StringBuilder();
		int tableCount = tableNames.size();
		for(int i=0; i<tableCount; i++){
			if(i == tableCount - 1){
				result.append(tableNames.get(i));
			}
			else{
				result.append(tableNames.get(i) + ", ");
			}
		}
		return result.toString();
	}
	
	public JsonObject getAllTableNamesJson(){
		JsonObjectBuilder builder = Json.createObjectBuilder();
		if(tableNames.size() == 0){
			return builder.add("Result", "No Tables In DataBase").build();
		}
		JsonArrayBuilder tables = Json.createArrayBuilder();
		int tableCount = tableNames.size();
		for(int i=0; i<tableCount; i++){
			tables.add(tableNames.get(i));
		}
		return builder.add("Result", tables).build();
	}
	
	public void addRowsToCache(ArrayList<Row> rows){
		for(Row row : rows){
			cache.add(row);
		}
	}
}
