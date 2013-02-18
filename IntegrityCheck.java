package test;

import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;

public class IntegrityCheck {
	
	static ArrayList<String> tableList = new ArrayList<String>();
	static ArrayList<String> viewList = new ArrayList<String>();
	static ArrayList<String> commonTables = new ArrayList<String>();
	static ArrayList<String> commonViews = new ArrayList<String>();
	static ArrayList<String> differentFieldDefinition = new ArrayList<String>();
	
	public static void main(String args[]){
		
		System.out.println("-----------Connecting to PostgreSQL----------");
 
		try {
 
			Class.forName("org.postgresql.Driver");
 
		} catch (ClassNotFoundException e) {
 
			System.out.println("Driver not found! Include driver in library path.");
			e.printStackTrace();
			return;
 
		}
 
		System.out.println("PostgreSQL JDBC Driver Registered!");
 
		Connection connection1 = null;
		Connection connection2 = null;
		
		try {
			 
			connection1 = DriverManager.getConnection(
					"jdbc:postgresql://127.0.0.1:5432/globe", "postgres",
					"postpass");
			connection2 = DriverManager.getConnection(
					"jdbc:postgresql://127.0.0.1:5432/testingGlobe", 
					"postgres", "postpass");
 
		} catch (SQLException e) {
 
			System.out.println("Connection for one of the databases Failed!");
			e.printStackTrace();
			return;
 
		}
 
		if (connection1 != null && connection2 != null) {
			System.out.println("You made it to the both databases!");
		} else {
			System.out.println("Failed to make connection to either database!");
		}
		
		compareTables(connection1, connection2);
		compareViews(connection1, connection2);
		compareTableFields(connection1, connection2, "users");
		compareViewFields(connection1, connection2, "validated_cases");
		compareTableFieldDefinition(connection1, connection2, "users");
		compareViewFieldDefinition(connection1, connection2, "validated_cases");
		
	}
	
	//list all the tables that are present in one database but not in the other
	static void compareTables(Connection connection1, Connection connection2){
		PreparedStatement pst1 = null;
		ResultSet rs1 = null;
		PreparedStatement pst2 = null;
		ResultSet rs2 = null;
		try {
			String dbTablesList = "SELECT table_name FROM INFORMATION_SCHEMA.TABLES where table_schema = 'public'";
			pst1 = connection1.prepareStatement(dbTablesList,ResultSet.TYPE_SCROLL_INSENSITIVE,
					ResultSet.CONCUR_UPDATABLE);
			rs1 = pst1.executeQuery();
			pst2 = connection2.prepareStatement(dbTablesList,ResultSet.TYPE_SCROLL_INSENSITIVE,
					ResultSet.CONCUR_UPDATABLE);
			rs2 = pst2.executeQuery();
			while (rs1.next()) { 
				int counter = 0;
				while (rs2.next()){ 
					String str1 = rs1.getString(1);
					String str2 = rs2.getString(1);
					if(str1.equals(str2)){
						commonTables.add(rs1.getString(1)); //System.out.println("HEY!!!" + rs1.getString(1));
						counter++;
					}
				}
				if (counter == 0) 
					tableList.add(rs1.getString(1));
				rs2.beforeFirst();
			}
			while (rs2.next()){ 
				String str2 = rs2.getString(1);
				int counter = 0;
				for(int y = 0; y < commonTables.size(); y++){					
					if(commonTables.get(y).equals(str2)){
						counter++;
					}		
				}
				if (counter == 0) 
					tableList.add(rs2.getString(1));
			}
			
			System.out.println("\n list of all the tables that are present in one database but not in the other \n");
			
			for(int i = 0; i < tableList.size(); i++)
				System.out.println(tableList.get(i));
		} catch (SQLException e){
			System.out.println("Cannot query the either one of the databases");
			e.printStackTrace();
			return;
		}
	}
	
	//list all the views that are present in one database but not in the other
	static void compareViews(Connection connection1, Connection connection2){
		PreparedStatement pstViews1 = null;
		ResultSet rsViews1 = null;
		PreparedStatement pstViews2 = null;
		ResultSet rsViews2 = null;
		try {
			String dbViewsList = "SELECT table_name FROM INFORMATION_SCHEMA.views where table_schema = 'public'";
			pstViews1 = connection1.prepareStatement(dbViewsList,ResultSet.TYPE_SCROLL_INSENSITIVE,
					ResultSet.CONCUR_UPDATABLE);
			rsViews1 = pstViews1.executeQuery();
			pstViews2 = connection2.prepareStatement(dbViewsList,ResultSet.TYPE_SCROLL_INSENSITIVE,
					ResultSet.CONCUR_UPDATABLE);
			rsViews2 = pstViews2.executeQuery();
			while (rsViews1.next()) {
				int count = 0;
				while (rsViews2.next()){
					if(rsViews1.getString(1).equals(rsViews2.getString(1))){
						commonViews.add(rsViews1.getString(1));
						count++;
					}
				}
				if (count == 0) 
					viewList.add(rsViews1.getString(1));
				rsViews2.beforeFirst();
			}
			
			while (rsViews2.next()){ 
				String str2 = rsViews2.getString(1);
				int counter = 0;
				for(int y = 0; y < commonViews.size(); y++){					
					if(commonViews.get(y).equals(str2)){
						counter++;
					}		
				}
				if (counter == 0) 
					viewList.add(rsViews2.getString(1));
			}
			
			System.out.println("\n list of all the views that are present in one database but not in the other \n");
			
			for(int i = 0; i < viewList.size(); i++)
				System.out.println(viewList.get(i));
		} catch (SQLException e){
			System.out.println("Cannot query the either one of the databases");
			e.printStackTrace();
			return;
		}
		
	}
	
	//compare the fields in all the tables that are present in both
	static void compareTableFields(Connection connection1, Connection connection2, String tableName){
		
		int counter = 0;
		ArrayList<String> exclusiveFields = new ArrayList<String>();
		ArrayList<String> commonFields = new ArrayList<String>();
		
		for(int j = 0; j<commonTables.size(); j++){
			if(commonTables.get(j).equals(tableName))
				counter++;
		}
		
		if(counter != 0){
			try {
				
				PreparedStatement pstTableFields1 = null;
				ResultSet rsTableFields1 = null;
				PreparedStatement pstTableFields2 = null;
				ResultSet rsTableFields2 = null;
				String dbTableFields = "select column_name from information_schema.columns where table_name = ?";
				pstTableFields1 = connection1.prepareStatement(dbTableFields,ResultSet.TYPE_SCROLL_INSENSITIVE,
						ResultSet.CONCUR_UPDATABLE);
				pstTableFields1.setString(1, tableName);
				rsTableFields1 = pstTableFields1.executeQuery();
				pstTableFields2 = connection2.prepareStatement(dbTableFields,ResultSet.TYPE_SCROLL_INSENSITIVE,
						ResultSet.CONCUR_UPDATABLE);
				pstTableFields2.setString(1, tableName);
				rsTableFields2 = pstTableFields2.executeQuery();
				
				//list the fields that are present in one db's table but not in the other
				while (rsTableFields1.next()) {
					int countFields = 0;
					while (rsTableFields2.next()){
						if(rsTableFields1.getString(1).equals(rsTableFields2.getString(1))){
							commonFields.add(rsTableFields1.getString(1));
							countFields++;
						}
					}
					if (countFields == 0) 
						exclusiveFields.add(rsTableFields1.getString(1));
					rsTableFields2.beforeFirst();
				}
				
				while (rsTableFields2.next()){ 
					String str2 = rsTableFields2.getString(1);
					int count = 0;
					for(int y = 0; y < commonFields.size(); y++){					
						if(commonFields.get(y).equals(str2)){
							count++;
						}		
					}
					if (count == 0) 
						exclusiveFields.add(rsTableFields2.getString(1));
				}
				
			} catch (SQLException e) {
				e.printStackTrace();
				return;
			}
			
			System.out.println("\n list of all the fields that are exclusive \n");
			
			for(int i = 0; i < exclusiveFields.size(); i++)
				System.out.println(exclusiveFields.get(i));
			
		}
		else 
			System.out.println("ERROR: The given table is not common.");
		
	}
	
	//compare the fields in all the views that are present in both
	static void compareViewFields(Connection connection1, Connection connection2, String viewName){
		
		int counter = 0;
		ArrayList<String> exclusiveFields = new ArrayList<String>();
		ArrayList<String> commonFields = new ArrayList<String>();
		
		for(int j = 0; j<commonViews.size(); j++){
			if(commonViews.get(j).equals(viewName))
				counter++;
		}
		
		if(counter != 0){
			try {
				
				PreparedStatement pstViewFields1 = null;
				ResultSet rsViewFields1 = null;
				PreparedStatement pstViewFields2 = null;
				ResultSet rsViewFields2 = null;
				String dbViewFields = "select column_name from information_schema.columns where table_name = ?";
				pstViewFields1 = connection1.prepareStatement(dbViewFields,ResultSet.TYPE_SCROLL_INSENSITIVE,
						ResultSet.CONCUR_UPDATABLE);
				pstViewFields1.setString(1, viewName);
				rsViewFields1 = pstViewFields1.executeQuery();
				pstViewFields2 = connection2.prepareStatement(dbViewFields,ResultSet.TYPE_SCROLL_INSENSITIVE,
						ResultSet.CONCUR_UPDATABLE);
				pstViewFields2.setString(1, viewName);
				rsViewFields2 = pstViewFields2.executeQuery();
				
				//list the fields that are present in one db's table but not in the other
				while (rsViewFields1.next()) {
					int countFields = 0;
					while (rsViewFields2.next()){
						if(rsViewFields1.getString(1).equals(rsViewFields2.getString(1))){
							commonFields.add(rsViewFields1.getString(1));
							countFields++;
						}
					}
					if (countFields == 0) 
						exclusiveFields.add(rsViewFields1.getString(1));
					rsViewFields2.beforeFirst();
				}
				
				while (rsViewFields2.next()){ 
					String str2 = rsViewFields2.getString(1);
					int count = 0;
					for(int y = 0; y < commonFields.size(); y++){					
						if(commonFields.get(y).equals(str2)){
							count++;
						}		
					}
					if (count == 0) 
						exclusiveFields.add(rsViewFields2.getString(1));
				}
				
			} catch (SQLException e) {
				e.printStackTrace();
				return;
			}
			
			System.out.println("\n list of all the fields that are exclusive in views: \n");
			
			for(int i = 0; i < exclusiveFields.size(); i++)
				System.out.println(exclusiveFields.get(i));
			
		}
		else 
			System.out.println("ERROR: The given table is not common.");
		
	}
	
	//compare the fields in all the tables that are present in both
	static void compareTableFieldDefinition(Connection connection1, Connection connection2, String tableName){
		
		int counter = 0;
		ArrayList<String> exclusiveFieldDefinition = new ArrayList<String>();
		
		for(int j = 0; j<commonTables.size(); j++){
			if(commonTables.get(j).equals(tableName))
				counter++;
		}
		
		if(counter != 0){
			try {
				
				PreparedStatement pstTableFieldsDef1 = null;
				ResultSet rsTableFieldsDef1 = null;
				PreparedStatement pstTableFieldsDef2 = null;
				ResultSet rsTableFieldsDef2 = null;
				String dbTableFieldsDef = "select column_name, data_type from information_schema.columns where table_name = ?";
				pstTableFieldsDef1 = connection1.prepareStatement(dbTableFieldsDef,ResultSet.TYPE_SCROLL_INSENSITIVE,
						ResultSet.CONCUR_UPDATABLE);
				pstTableFieldsDef1.setString(1, tableName);
				rsTableFieldsDef1 = pstTableFieldsDef1.executeQuery();
				pstTableFieldsDef2 = connection2.prepareStatement(dbTableFieldsDef,ResultSet.TYPE_SCROLL_INSENSITIVE,
						ResultSet.CONCUR_UPDATABLE);
				pstTableFieldsDef2.setString(1, tableName);
				rsTableFieldsDef2 = pstTableFieldsDef2.executeQuery();
				
				//list the fields that are present in one db's table but not in the other
				while (rsTableFieldsDef1.next()) {
					while (rsTableFieldsDef2.next()){
						if(rsTableFieldsDef1.getString(1).equals(rsTableFieldsDef2.getString(1))){
							if(!rsTableFieldsDef1.getString(2).equals(rsTableFieldsDef2.getString(2)))
								exclusiveFieldDefinition.add(rsTableFieldsDef1.getString(1));
						}
					}
					rsTableFieldsDef2.beforeFirst();
				}
				
			} catch (SQLException e) {
				e.printStackTrace();
				return;
			}
			
			System.out.println("\n list of all the field defs that are exclusive in table \n");
			
			for(int i = 0; i < exclusiveFieldDefinition.size(); i++)
				System.out.println(exclusiveFieldDefinition.get(i));
			
		}
		else 
			System.out.println("ERROR: The given table is not common.");
		
	}
	
	//compare the fields in all the views that are present in both
	static void compareViewFieldDefinition(Connection connection1, Connection connection2, String viewName){
		
		int counter = 0;
		ArrayList<String> exclusiveFieldDefinition = new ArrayList<String>();
		
		for(int j = 0; j<commonViews.size(); j++){
			if(commonViews.get(j).equals(viewName))
				counter++;
		}
		
		if(counter != 0){
			try {
				
				PreparedStatement pstViewFieldsDef1 = null;
				ResultSet rsViewFieldsDef1 = null;
				PreparedStatement pstViewFieldsDef2 = null;
				ResultSet rsViewFieldsDef2 = null;
				String dbViewFieldsDef = "select column_name, data_type from information_schema.columns where table_name = ?";
				pstViewFieldsDef1 = connection1.prepareStatement(dbViewFieldsDef,ResultSet.TYPE_SCROLL_INSENSITIVE,
						ResultSet.CONCUR_UPDATABLE);
				pstViewFieldsDef1.setString(1, viewName);
				rsViewFieldsDef1 = pstViewFieldsDef1.executeQuery();
				pstViewFieldsDef2 = connection2.prepareStatement(dbViewFieldsDef,ResultSet.TYPE_SCROLL_INSENSITIVE,
						ResultSet.CONCUR_UPDATABLE);
				pstViewFieldsDef2.setString(1, viewName);
				rsViewFieldsDef2 = pstViewFieldsDef2.executeQuery();
				
				//list the fields that are present in one db's table but not in the other
				while (rsViewFieldsDef1.next()) {
					while (rsViewFieldsDef2.next()){
						if(rsViewFieldsDef1.getString(1).equals(rsViewFieldsDef2.getString(1))){
							if(!rsViewFieldsDef1.getString(2).equals(rsViewFieldsDef2.getString(2)))
								exclusiveFieldDefinition.add(rsViewFieldsDef1.getString(1));
						}
					}
					rsViewFieldsDef2.beforeFirst();
				}
				
			} catch (SQLException e) {
				e.printStackTrace();
				return;
			}
			
			System.out.println("\n list of all the field defs that are exclusive in view \n");
			
			for(int i = 0; i < exclusiveFieldDefinition.size(); i++)
				System.out.println(exclusiveFieldDefinition.get(i));
			
		}
		else 
			System.out.println("ERROR: The given table is not common.");
		
	}

}
