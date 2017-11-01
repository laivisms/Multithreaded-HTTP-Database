package edu.os.database.DataBase;


import java.io.BufferedReader;
//import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
//import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
//import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

public class APIServer {

private static final String DBurl = "http://127.0.0.1:1242/query";
private static final int portNum = 1234;
private final static ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(35);
	
	public static void main(String[] args) throws IOException{
		
		InetSocketAddress port = new InetSocketAddress(portNum);
		HttpServer server = HttpServer.create(port, 0);
		server.createContext("/pingAPI", new pingHandler());
		server.createContext("/querySend", new queryHandler());
        server.setExecutor(scheduler);
        server.start();
        
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
					    //System.out.println(result.toString() + "API");
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
			 StringBuilder DBresult = null;
				String response = null;
			    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
			    StringBuilder result = new StringBuilder();
			    String line = null;
			    try {
					while((line = reader.readLine()) != null) {
					    result.append(line);
					    //System.out.println(result.toString() + "API");
					}
			
			
		     String query = result.toString();
		     
			     URL DataBaseServer = new URL(DBurl);
					HttpURLConnection connection = (HttpURLConnection) DataBaseServer.openConnection();
				    connection.setDoOutput(true);
				    connection.setRequestProperty("Accept-Charset", "UTF-8");
				    connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded;charset=UTF-8");
				    OutputStream output = connection.getOutputStream();
				    
				
			    
					output.write(query.getBytes("UTF-8"));
					
					
					InputStream inputDB = connection.getInputStream();
				    BufferedReader DBreader = new BufferedReader(new InputStreamReader(inputDB));
				    DBresult = new StringBuilder();
				    
				    String DBline = null;
					while((DBline = DBreader.readLine()) != null) {
					    DBresult.append(DBline);
					    System.out.println(DBresult.toString());
					}
				} catch (IOException e) {
					response = e.getMessage();
					ex.sendResponseHeaders(400, response.length());
				     OutputStream os = ex.getResponseBody();
				     os.write(response.getBytes());
				     os.close();
				}
		    if(response == null){
		    	response = DBresult.toString();
			     ex.sendResponseHeaders(200, response.length());
			     OutputStream os = ex.getResponseBody();
			     os.write(response.getBytes());
			     os.close();
		    }
			
		}
		
	}
	
	
}