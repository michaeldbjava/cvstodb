package de.rib.readcsv;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.Reader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import com.mysql.jdbc.PreparedStatement;

import de.rib.datehelper.ConvertDateToIso;

public class ImportCSVToDb {

	public static void main(String[] args) {
		String pathOfConfigFile = args[0];
		boolean configFileExists = false;
		/*
		 * If first argument is an path to config file then use it. In other
		 * case use default file
		 */
		if (args.length != 0) {
			Path path = Paths.get(pathOfConfigFile);
			System.out.println("Übergebener Pfad: " + pathOfConfigFile);
			configFileExists = Files.exists(path, new LinkOption[] { LinkOption.NOFOLLOW_LINKS });
			System.out.println("Config File Exists: " + configFileExists);
		}
		// Als erstes wird ein Objekt vom Typ ConfigurationCvsToDB erstellt.
		ConfigurationCsvToDB cvsDBConfig = new ConfigurationCsvToDB();
		if (configFileExists == true) {
			System.out.println("Lese Config Datei: " + pathOfConfigFile);
			cvsDBConfig.readConfigFile(pathOfConfigFile);
		} else {
			System.out.println("Lese Standard Config Datei");
			cvsDBConfig.readConfigFile("csvtodb_config.xml");
		}

		Connection con = cvsDBConfig.getConnectionToDb();
		// TODO Auto-generated method stub
		try {
			// Example CVS File must be places in project dircetory

			/*
			 * Als erstes Konfigurationsdatei auslesen (mit einem kleinen DOM
			 * Parser
			 */

			ArrayList<FieldCSVToDb> listOfFields = cvsDBConfig.getMapList();

			File file = new File(cvsDBConfig.getCvsfile());

			CSVFormat csvFormat = CSVFormat.newFormat(cvsDBConfig.getDelimeter()).withRecordSeparator("\n")
					.withFirstRecordAsHeader();

			CSVParser parser = CSVParser.parse(file, StandardCharsets.UTF_8, csvFormat);
			Iterable<CSVRecord> records = parser.getRecords();
			String columnList = "(";
			String columnUpdateList = " ";
			DatabaseMetaData dmD = con.getMetaData();
			ResultSet rsPrimaryKey = dmD.getPrimaryKeys(null, null, cvsDBConfig.getTable());
			List<String> primaryKeyList = new ArrayList<String>();
			while(rsPrimaryKey.next()){
				String columnNamePK = rsPrimaryKey.getString(4);
				primaryKeyList.add(columnNamePK);
			}
			
			/* Erzeuge eine Spalten Liste, für die Insert Anweisung*/
			for (int i=0;i<listOfFields.size();i++) {
				FieldCSVToDb fCvsDb=listOfFields.get(i);
				if(i==0){
					columnList = columnList + fCvsDb.getDbField();
					
				}
				else{
					columnList = columnList + "," + fCvsDb.getDbField();
					
				}
				

			}
			
			/* Liste definieren, die ausschließlich Felder aufnimmt, die aktualisiert werden sollen. Diese Liste darf keine Primärschlüsselspalten enthalten*/
			List<FieldCSVToDb> updateFields = new ArrayList<FieldCSVToDb>();
			for(int i=0;i<listOfFields.size();i++){
				FieldCSVToDb fCvsDb = listOfFields.get(i);
				if(!primaryKeyList.contains(fCvsDb.getDbField())){
					updateFields.add(fCvsDb);
				}
			}
			

			/* Zusammensetzen der Wertzuweisungen (=?) der Update Spalten, für das PreparedStatement */
			for(int i=0;i<updateFields.size();i++){
				FieldCSVToDb fCvsDb2 = updateFields.get(i);
				if(i==0){
					columnUpdateList = columnUpdateList + fCvsDb2.getDbField()+ "=?";
				}
				else{
					columnUpdateList = columnUpdateList +  "," + fCvsDb2.getDbField()+ "=?";
				}
			}
				
			
			
			
			
			//columnUpdateList=columnUpdateList.replaceFirst(",", "");
			columnList = columnList + ")";
			System.out.println(columnUpdateList);

			int listOfFieldsLength = listOfFields.size();
			String placeholderValues = "";
			for (int i = 0; i < listOfFieldsLength; i++) {
				if (i < listOfFieldsLength - 1)
					placeholderValues = placeholderValues + "?,";
				else {
					placeholderValues = placeholderValues + "?";
				}

			}
			/* 
			 * Hier werden die Feld Ausdrücke, die in der Konfigurationsdatei angegeben werden, zusammengebaut.
			 * Da ich hier jetzt Prepared Statements verwende, müssen diese zunächst mit in die ? Stellvertreterlist für Werte mit aufgenommen werden.
			 * 
			 * Das Gleiche gilt natürlich für die Komma separierte Liste der Spaltennamen.
			 */
			ArrayList<FieldEXPRESSIONToDB> listExpressions = cvsDBConfig.getMapListExpressions();
			/*if (listExpressions != null) {
				for (int i = 0; i < listExpressions.size(); i++) {
					FieldEXPRESSIONToDB fETDB = listExpressions.get(i);
					if (i == 0) {
						columnList = columnList + "," + fETDB.getTableColumn();
						placeholderValues=placeholderValues+",?";
					} else {
						columnList = columnList + "," + fETDB.getTableColumn();
						placeholderValues=placeholderValues+",?";
					}
					
				}
			}
			*/
			String sqlInstert = "INSERT INTO " + cvsDBConfig.getTable() + " " + columnList + " VALUES ("
					+ placeholderValues + ") ON DUPLICATE KEY UPDATE " + columnUpdateList;
			System.out.println(sqlInstert);
			java.sql.PreparedStatement prepStatement = con.prepareStatement(sqlInstert);

			
			
			// System.out.println("Column List: " + columnList);

			// System.out.println(columnList);
			
			TypMetaInformation tMI = new TypMetaInformation();
			con.setAutoCommit(false);
			for (CSVRecord record : records) {
				
				int j=0;
				for (int i = 0; i < listOfFields.size(); i++) {
					j++;
					
					/*if(i==13)
						System.out.println("i ist gleich 13");*/
					FieldCSVToDb fCvsDb = listOfFields.get(i);

					int typOfColumn = fCvsDb.getType();

					String value = record.get(fCvsDb.getCvsField());
					/*System.out.println("Wert von j: " + j + " Wert aus Zeile:" + value );*/

					switch (typOfColumn) {
					case Types.ARRAY:
						System.out.println("Array Typ is not supported");
						break;
					case Types.BIGINT:
						prepStatement.setInt(j, Integer.parseInt(value));
						break;
					case Types.BINARY:
						System.out.println("Binary Typ is not supported");
						break;
					case Types.BIT:
						prepStatement.setBoolean(j, Boolean.parseBoolean(value));
						break;
					case Types.BLOB:
						System.out.println("Blob Typ is not supported");
						break;
					case Types.BOOLEAN:
						prepStatement.setBoolean(j, Boolean.parseBoolean(value));
						break;
					case Types.CHAR:
						prepStatement.setString(j, value);
						break;
					case Types.CLOB:
						System.out.println("CLOB Typ is not supported");
						break;
					case Types.DATALINK:
						System.out.println("DATALINK Typ is not supported");
						break;
					case Types.DATE:
						String pattern = "yyyy-MM-dd";
						SimpleDateFormat formatOfDate = new SimpleDateFormat(pattern);
						prepStatement.setDate(j,new java.sql.Date(formatOfDate.parse(value).getTime()) );
						break;
					case Types.DECIMAL:
						prepStatement.setBigDecimal(j,BigDecimal.valueOf(Double.parseDouble(value.replace(',', '.'))));
						break;
					case Types.DISTINCT:
						System.out.println("DISTINCT Typ is not supported");
						break;
					case Types.DOUBLE:
						prepStatement.setDouble(j,Double.parseDouble(value));
						break;
					case Types.FLOAT:
						prepStatement.setFloat(j,Float.parseFloat(value));
						break;
					case Types.INTEGER:
						prepStatement.setInt(j,Integer.parseInt(value));
						break;
					case Types.JAVA_OBJECT:
						System.out.println("JAVA_OBJECT Typ is not supported");
						break;
					case Types.LONGNVARCHAR:
						prepStatement.setString(j,value);
						break;
					case Types.LONGVARBINARY:
						prepStatement.setString(j,value);
						break;
					case Types.LONGVARCHAR:
						prepStatement.setString(j,value);
						break;
					case Types.NCHAR:
						prepStatement.setString(j,value);
						break;
					case Types.NCLOB:
						prepStatement.setString(j,value);
						break;
					case Types.NULL:
						System.out.println("NULL Typ is not supported");;
						break;
					case Types.NUMERIC:
						prepStatement.setDouble(j,Double.parseDouble(value));
						break;
					case Types.NVARCHAR:
						prepStatement.setString(j,value);
						break;
					case Types.OTHER:
						System.out.println("OTHER Typ is not supported");
						break;
					case Types.REAL:
						System.out.println("REAL Typ is not supported");
						break;	
					case Types.REF:
						System.out.println("REF Typ is not supported");
						break;
					case Types.REF_CURSOR:
						System.out.println("REF_CURSOR Typ is not supported");
						break;					
					case Types.ROWID:
						System.out.println("ROWID Typ is not supported");
						break;
					case Types.SMALLINT:
						prepStatement.setInt(j,Integer.parseInt(value));
						break;
					case Types.SQLXML:
						System.out.println("SQLXML Typ is not supported");
						break;
					case Types.STRUCT:
						System.out.println("STRUCT Typ is not supported");
						break;	
					case Types.TIME:
						prepStatement.setTime(j,new java.sql.Time(Integer.parseInt(value)));
						break;	
					case Types.TIME_WITH_TIMEZONE:
						prepStatement.setTime(j,new java.sql.Time(Integer.parseInt(value)));
						break;	
					case Types.TIMESTAMP:
						SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
						prepStatement.setTimestamp(j,new Timestamp(dateFormat.parse(value).getTime()));
						break;
					case Types.TIMESTAMP_WITH_TIMEZONE:
						prepStatement.setTimestamp(j,new java.sql.Timestamp(Long.parseLong(value)));
						break;
					case Types.TINYINT:
						prepStatement.setInt(j,Integer.parseInt(value));
						break;
					case Types.VARBINARY:
						System.out.println("VARBINARY Typ is not supported");
						break;
					case Types.VARCHAR:
						prepStatement.setString(j,value);
						break;
					
					}
				}
				
				
				/* Das müssen wir uns noch mal gut Ausdrücke, die in der XML Datei angegeben werden, 
				 * lassen sich wahrscheinlich nicht in PreparedStatements verwenden. Insbesondere dann, 
				 * wenn wir hier Funktionsaufrufe festlegen. 
				 * for(int i=0;i<listExpressions.size();i++){
					j++;
					FieldEXPRESSIONToDB fEXToDb = listExpressions.get(i);
					
				}*/
				
				
				/* 
				 * Ich muss mir die Felder, die Übrigbleiben holen und in der Reihenfolge einfügen, wie Sie in den Wertzuweisungen vorliegen
				 * Ich muss zunächst die Spalten ermitteln, die nicht Primäschlüsselspalten sind.
				 * */
				for(int i=0;i<updateFields.size();i++){
					j++;
					FieldCSVToDb fCvsDb = updateFields.get(i);
					int typOfColumn  = fCvsDb.getType();


					String value = record.get(fCvsDb.getCvsField());
					/*System.out.println("Wert von j: " + j + " Wert aus Zeile:" + value );*/

					switch (typOfColumn) {
					case Types.ARRAY:
						System.out.println("Array Typ is not supported");
						break;
					case Types.BIGINT:
						prepStatement.setInt(j, Integer.parseInt(value));
						break;
					case Types.BINARY:
						System.out.println("Binary Typ is not supported");
						break;
					case Types.BIT:
						prepStatement.setBoolean(j, Boolean.parseBoolean(value));
						break;
					case Types.BLOB:
						System.out.println("Blob Typ is not supported");
						break;
					case Types.BOOLEAN:
						prepStatement.setBoolean(j, Boolean.parseBoolean(value));
						break;
					case Types.CHAR:
						prepStatement.setString(j, value);
						break;
					case Types.CLOB:
						System.out.println("CLOB Typ is not supported");
						break;
					case Types.DATALINK:
						System.out.println("DATALINK Typ is not supported");
						break;
					case Types.DATE:
						String pattern = "yyyy-MM-dd";
						SimpleDateFormat formatOfDate = new SimpleDateFormat(pattern);
						prepStatement.setDate(j,new java.sql.Date(formatOfDate.parse(value).getTime()) );
						break;
					case Types.DECIMAL:
						prepStatement.setBigDecimal(j,BigDecimal.valueOf(Double.parseDouble(value.replace(',', '.'))));
						break;
					case Types.DISTINCT:
						System.out.println("DISTINCT Typ is not supported");
						break;
					case Types.DOUBLE:
						prepStatement.setDouble(j,Double.parseDouble(value));
						break;
					case Types.FLOAT:
						prepStatement.setFloat(j,Float.parseFloat(value));
						break;
					case Types.INTEGER:
						prepStatement.setInt(j,Integer.parseInt(value));
						break;
					case Types.JAVA_OBJECT:
						System.out.println("JAVA_OBJECT Typ is not supported");
						break;
					case Types.LONGNVARCHAR:
						prepStatement.setString(j,value);
						break;
					case Types.LONGVARBINARY:
						prepStatement.setString(j,value);
						break;
					case Types.LONGVARCHAR:
						prepStatement.setString(j,value);
						break;
					case Types.NCHAR:
						prepStatement.setString(j,value);
						break;
					case Types.NCLOB:
						prepStatement.setString(j,value);
						break;
					case Types.NULL:
						System.out.println("NULL Typ is not supported");;
						break;
					case Types.NUMERIC:
						prepStatement.setDouble(j,Double.parseDouble(value));
						break;
					case Types.NVARCHAR:
						prepStatement.setString(j,value);
						break;
					case Types.OTHER:
						System.out.println("OTHER Typ is not supported");
						break;
					case Types.REAL:
						System.out.println("REAL Typ is not supported");
						break;	
					case Types.REF:
						System.out.println("REF Typ is not supported");
						break;
					case Types.REF_CURSOR:
						System.out.println("REF_CURSOR Typ is not supported");
						break;					
					case Types.ROWID:
						System.out.println("ROWID Typ is not supported");
						break;
					case Types.SMALLINT:
						prepStatement.setInt(j,Integer.parseInt(value));
						break;
					case Types.SQLXML:
						System.out.println("SQLXML Typ is not supported");
						break;
					case Types.STRUCT:
						System.out.println("STRUCT Typ is not supported");
						break;	
					case Types.TIME:
						prepStatement.setTime(j,new java.sql.Time(Integer.parseInt(value)));
						break;	
					case Types.TIME_WITH_TIMEZONE:
						prepStatement.setTime(j,new java.sql.Time(Integer.parseInt(value)));
						break;	
					case Types.TIMESTAMP:
						SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
						prepStatement.setTimestamp(j,new Timestamp(dateFormat.parse(value).getTime()));
						break;
					case Types.TIMESTAMP_WITH_TIMEZONE:
						prepStatement.setTimestamp(j,new java.sql.Timestamp(Long.parseLong(value)));
						break;
					case Types.TINYINT:
						prepStatement.setInt(j,Integer.parseInt(value));
						break;
					case Types.VARBINARY:
						System.out.println("VARBINARY Typ is not supported");
						break;
					case Types.VARCHAR:
						prepStatement.setString(j,value);
						break;
					
					}
					
					
				}
												
				prepStatement.addBatch();
				
				
				
				
			}

			prepStatement.executeBatch();
			con.commit();
			con.close();
			
			


		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(e.getMessage());
			try {
				con.rollback();
			} catch (Exception e1) {

			}
		}

	}

}
