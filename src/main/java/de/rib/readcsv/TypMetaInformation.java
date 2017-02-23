package de.rib.readcsv;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

public class TypMetaInformation {
	private Connection con;
	public TypMetaInformation() {
		// TODO Auto-generated constructor stub
	}
	public boolean isNumeric(String tableName,String columnName,Connection con){
		DatabaseMetaData dBM = null;
		boolean numeric = false;
		this.con=con;
		try{
//			System.out.println("Verbindung in TypMetaInformation ist Null: " + con==null);
//			System.out.println("Verbindung in geschlossen: " + con.isClosed());
			dBM=con.getMetaData();
			ResultSet rsColumnMeta = dBM.getColumns(null, null, tableName, columnName);
			rsColumnMeta.next();
			int typeOfColumn = rsColumnMeta.getInt(5);
			
			if(typeOfColumn==Types.BIGINT 
					|| typeOfColumn==Types.DECIMAL 
					|| typeOfColumn==Types.DOUBLE 
					|| typeOfColumn==Types.FLOAT 
					|| typeOfColumn==Types.INTEGER
					|| typeOfColumn==Types.NUMERIC
					|| typeOfColumn==Types.REAL
					|| typeOfColumn==Types.SMALLINT
					|| typeOfColumn==Types.TINYINT){
				numeric=true;
				
			}
			else{
				numeric=false;
			}
			/*System.out.println("Bigint: " + Types.BIGINT);
			System.out.println("Boolean: " + Types.BOOLEAN); 
			System.out.println("Char: " + Types.CHAR); 
			System.out.println("Date: " + Types.DATE); 
			System.out.println("Decimal: " + Types.DECIMAL);
			System.out.println("Double: " + Types.DOUBLE);
			System.out.println("Float: " + Types.FLOAT);
			System.out.println("Integer: " + Types.INTEGER);
			System.out.println("Longnvarchar: " + Types.LONGNVARCHAR);
			System.out.println("Longvarchar: " + Types.LONGVARCHAR);
			System.out.println("Nchar: " + Types.NCHAR);
			System.out.println("Numeric: " + Types.NUMERIC);
			System.out.println("Nvarchar: " + Types.NVARCHAR);
			System.out.println("Real: " + Types.REAL);
			System.out.println("Smallint: " + Types.SMALLINT);
			System.out.println("Time: " + Types.TIME);
			System.out.println("Tinyint: " + Types.TINYINT);
			System.out.println("Varchar: " + Types.VARCHAR);
			*/
			
		}
		catch(SQLException sqlException){
			
		}
		return numeric;
	}
	
	
	public boolean isDate(String tableName,String columnName,Connection con){
		DatabaseMetaData dBM = null;
		boolean datetype = false;
		this.con=con;
		try{
			dBM=con.getMetaData();
			ResultSet rsColumnMeta = dBM.getColumns(null, null, tableName, columnName);
			rsColumnMeta.next();
			int typeOfColumn = rsColumnMeta.getInt(5);
			
			if(typeOfColumn==Types.DATE) {
						datetype=true;
				
			}
			else{
				datetype=false;
			}
				
		}
		catch(SQLException sqlException){
			
		}
		return datetype;
	}
	
	public int getTyp(String tableName,String columnName,Connection con){
		DatabaseMetaData dBM = null;
		this.con=con;
		try{
//			System.out.println("Verbindung in TypMetaInformation ist Null: " + con==null);
//			System.out.println("Verbindung in geschlossen: " + con.isClosed());
			dBM=con.getMetaData();
			ResultSet rsColumnMeta = dBM.getColumns(null, null, tableName, columnName);
			rsColumnMeta.next();
			int typeOfColumn = rsColumnMeta.getInt(5);
			return typeOfColumn;
		}
		catch(SQLException e){
			e.printStackTrace();
			return 0;
		}
		
	}

}
