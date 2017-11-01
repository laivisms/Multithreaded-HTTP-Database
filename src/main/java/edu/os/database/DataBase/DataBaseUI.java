package edu.os.database.DataBase;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.StringReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Scanner;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonValue;
/**
 * this program can be run in the Command prompt to send requests to the server over HTTP, currently configured through local host, but can 
 * be changed to any IP
 * @author SL
 *
 */
public class DataBaseUI {
	private static final String url = "http://localhost:1234/querySend";
	public static void main(String[] args){
		
		Scanner scanner = new Scanner(System.in);
		System.out.println("Enter DataBase Query:");
		while(scanner.hasNextLine()){
			
			String query = scanner.nextLine();
			if(query.equals("close") || query.equals("exit")){
				break;
			}
			runQuery(query);
			System.out.println("Enter DataBase Query:");
		}
		scanner.close();
	}
	
	
	
	public static void runQuery(String query){
		
		try {
			
			URL toPing = new URL(url);
			HttpURLConnection connection = (HttpURLConnection) toPing.openConnection();
		    connection.setDoOutput(true);
		    connection.setRequestProperty("Accept-Charset", "UTF-8");
		    connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded;charset=UTF-8");
		    OutputStream output = connection.getOutputStream();
		
	    
			output.write(query.getBytes("UTF-8"));
	   
	    
			InputStream in = null;
			in = connection.getInputStream();
		    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
		    StringBuilder JSonString = new StringBuilder();
		    String line = null;
			while((line = reader.readLine()) != null) {
			    JSonString.append(line);
			    //System.out.println(result.toString());
			}
			JsonReader Jreader = Json.createReader(new StringReader(JSonString.toString()));
			JsonObject result = Jreader.readObject();
			if(result.containsKey("Result")){
				System.out.println("Result : " + result.get("Result").toString());
			}
			else{
				JsonArray columns = (JsonArray) result.get("Columns");
				for(JsonValue column : columns){
					System.out.print("|" + column.toString() + "|");
				}
				JsonArray rows = result.getJsonArray("Rows");
				for(JsonValue row : rows){
					System.out.println();
					for(JsonValue value : (JsonArray)row){
						System.out.print("|" + value.toString() + "|");
					}
				}
				System.out.println("\n_________________________________________");
			}
		} catch (IOException e) {
			
			e.printStackTrace();
		
		}
		
	}
}
