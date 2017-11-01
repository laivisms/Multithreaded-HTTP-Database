package edu.os.database.DataBase;

import java.io.StringReader;

import java.util.Scanner;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonValue;

public class DBServerUI {
	
	private static DBServer dbserver;
	
	public static void main(String[] args){
		
		dbserver = new DBServer();
		
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
		
			
			String JSonString = dbserver.parseQuery(query);
	   
	    
			
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

	}
}
		
	

