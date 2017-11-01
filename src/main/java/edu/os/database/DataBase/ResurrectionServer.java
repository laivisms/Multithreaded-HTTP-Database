package edu.os.database.DataBase;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
//import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
//import java.net.MalformedURLException;
//import java.net.Socket;
import java.net.URL;
//import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
//import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

/**
 * The main class of the server. starts the api server and DataBase server, then pings them periodically to ensure theyre still alive, and 
 * restarts them if theyre not
 * @author SL
 *
 */
public class ResurrectionServer {
	private static final String APIurl = "http://127.0.0.1:1234/pingAPI";
	private static final String DBurl = "http://127.0.0.1:1242/pingDB";
	private static final int portNumber = 987;
	//private final ExecutorService exec = Executors.newCachedThreadPool();
	private final static ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(35);

	
	public static void main(String[] args) throws IOException{
		//startAPI();
		//startDB();
		InetSocketAddress port = new InetSocketAddress(portNumber);
		
		HttpServer server = HttpServer.create(port, 0);
		server.createContext("/isDBAlive", new queryHandler());
        server.setExecutor(scheduler);
        server.start();
        ping();
	}
	
	public static void startAPI() throws IOException{
		APIServer.main(new String[0]);
	}
	
	public static void startDB() throws IOException{
		DBServer.main(new String[0]);
	}
	
	
	
	static class queryHandler implements HttpHandler{

		@Override
		public void handle(HttpExchange ex) throws IOException {
			
			//InputStream is = ex.getRequestBody();
			
			
			String response = "This is the response";
            ex.sendResponseHeaders(200, response.length());
            OutputStream os = ex.getResponseBody();
            os.write(response.getBytes());
            os.close();
			
		}
		
	}
	
	public static class APIPinger implements Runnable{
		String chars = "UTF-8";
		String url = null;
		
		public APIPinger(String url){
			this.url = url;
		}
		
		
		@Override
		public void run() {
			 
	
			try {
				
				URL toPing = new URL(url);
				HttpURLConnection connection = (HttpURLConnection) toPing.openConnection();
			    connection.setDoOutput(true);
			    connection.setRequestProperty("Accept-Charset", chars);
			    connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded;charset=" + chars);
			    OutputStream output = connection.getOutputStream();
			
		    
				output.write("ping".getBytes(chars));
		   
		    
				InputStream in = connection.getInputStream();
			    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
			    StringBuilder result = new StringBuilder();
			    String line = null;
				while((line = reader.readLine()) != null) {
				    result.append(line);
				    //System.out.println(result.toString());
				}
			} catch (IOException e) {
				try {
					ResurrectionServer.startAPI();
				} catch (IOException e1) {
					e1.printStackTrace();
				}
			}
		   
    	}
			
	}
	
	
	public static class DBPinger implements Runnable{
		String chars = "UTF-8";
		String url = null;
		
		public DBPinger(String url){
			this.url = url;
		}
		
		
		@Override
		public void run() {
			 
	
			try {
				
				URL toPing = new URL(url);
				HttpURLConnection connection = (HttpURLConnection) toPing.openConnection();
			    connection.setDoOutput(true);
			    connection.setRequestProperty("Accept-Charset", chars);
			    connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded;charset=" + chars);
			    OutputStream output = connection.getOutputStream();
		    
				output.write("ping".getBytes(chars));
		   
				InputStream in = connection.getInputStream();
			    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
			    StringBuilder result = new StringBuilder();
			    String line = null;
				while((line = reader.readLine()) != null) {
				    result.append(line);
				    //System.out.println(result.toString());
				}
			} catch (IOException e) {
				try {
					ResurrectionServer.startDB();
				} catch (IOException e1) {
					e1.printStackTrace();
				}
			}
		   
    	}
			
	}
		
	
	
	public static void ping() throws IOException{
		final Runnable APIpinger = new APIPinger(APIurl);
		final Runnable DBpinger = new DBPinger(DBurl);
		scheduler.scheduleAtFixedRate(APIpinger, 0, 1, TimeUnit.SECONDS);
		scheduler.scheduleAtFixedRate(DBpinger, 0, 1, TimeUnit.SECONDS);
		
		
	}
}
