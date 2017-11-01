package edu.os.database.DataBase;

//import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import edu.yu.cs.dataStructures.fall2016.SimpleSQLParser.SQLParser;
//import edu.yu.cs.dataStructures.fall2016.SimpleSQLParser.SQLQuery;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;




public class DBServer {
	private static int portNumber = 1242;
	private DataBase DB = new DataBase();
	SQLParser parser = new SQLParser();
	
	//private ReentrantReadWriteLock RWL = new ReentrantReadWriteLock(true); 
	private final static ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(100);
	private static DBServer dbserver = null;

	
	
	public static void main(String[] args) throws IOException{
		dbserver = new DBServer();
		HttpServer server = HttpServer.create(new InetSocketAddress(portNumber), 0);
		server.createContext("/query", new queryHandler());
		server.createContext("/pingDB", new pingHandler());
        server.setExecutor(scheduler);
        server.start();
	}
	
	public DBServer(){
		retreiveTables();
	}
	
	public static class stringHandle{
		public String output = null;
	}
	
	
	static class pingHandler implements HttpHandler{
		
		@Override
		public void handle(HttpExchange ex) throws IOException {
			InputStream in = ex.getRequestBody();
			
		    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
		    StringBuilder result = new StringBuilder();
		    String line = null;
		    try {
				while((line = reader.readLine()) != null) {
				    result.append(line);
				   // System.out.println(result.toString() + "db");
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			
		    
		    	
		    String response = "y";
		     ex.sendResponseHeaders(200, response.length());
		     OutputStream os = ex.getResponseBody();
		     os.write(response.getBytes());
		     os.close();
		     
			
			
		    
		}
		
	}
	
	static class queryHandler implements HttpHandler{

		@Override
		public void handle(HttpExchange ex) throws IOException {
			InputStream in = ex.getRequestBody();
			
		    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
		    StringBuilder result = new StringBuilder();
		    String line = null;
		    try {
				while((line = reader.readLine()) != null) {
				    result.append(line);
				    System.out.println(result.toString());
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		    
		    String query = result.toString();
		    String response = dbserver.parseQuery(query);
		    
		    //send the results back to the client
		    
		    
		    ex.sendResponseHeaders(200, response.length());
		     OutputStream os = ex.getResponseBody();
		     os.write(response.getBytes());
		     os.close();
		    
			
		}
		
	}
	
	
	public String parseQuery(String query){
		stringHandle table = new stringHandle();
		
		QueryHandler handler = new QueryHandler(DB, query, table);
		if(table.output == null){
			handler.run();
		}
		return table.output;
		
	}
	
	private void retreiveTables() {
		try {
			DB.retreiveTables();
		} catch (NullEntryException | DataTypeMismatchException | DuplicateEntryException | InvalidLengthException
				| IOException e) {
			e.printStackTrace();
			System.out.println("Error retreiving stored databases");
		}
	}
	
	void readLock(){
		DB.readLock();
	}
	
	void readUnlock(){
		DB.readUnlock();
	}
	
	void writeLock(){
		DB.writeLock();
	}
	
	void writeUnlock(){
		DB.writeUnlock();
	}
	
}