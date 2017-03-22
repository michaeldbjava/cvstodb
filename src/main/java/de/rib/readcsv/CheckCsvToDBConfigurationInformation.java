package de.rib.readcsv;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class CheckCsvToDBConfigurationInformation {
	private ConfigurationCsvToDB configCsvToDb;

	public CheckCsvToDBConfigurationInformation(ConfigurationCsvToDB configCsvToDb) {
		// TODO Auto-generated constructor stub
		this.configCsvToDb = configCsvToDb;
	}

	public static Map<String, String> validateInformation(ConfigurationCsvToDB configCsvToDb) {
		Map<String, String> errorMessageList = new HashMap<String, String>();
		boolean statusOfAllValidationInformation = false;
		String csvFileName = configCsvToDb.getCvsfile();
		String delimiter = String.valueOf(configCsvToDb.getDelimeter());
		String localhost = configCsvToDb.getLocalhost();
		String dbType = configCsvToDb.getDbtype();
		String database = configCsvToDb.getDatabase();
		String port = configCsvToDb.getPort();
		String user = configCsvToDb.getUser();
		String password = configCsvToDb.getPassword();
		String tableName = configCsvToDb.getTable();
		ArrayList<FieldCSVToDb> fieldToColumnList = configCsvToDb.getMapList();

		/* Prüfen, ob als Dateiname ein leere Zeichenkette übergeben wurde */
		if (csvFileName != null && csvFileName.trim().equals("")) {
			errorMessageList.put("noFileName", "Es wurde kein Dateiname für die zu importierende Datei angegeben!");
		}

		File file = new File(csvFileName);

		String pathOfDirectory = file.getAbsolutePath().substring(0,
				file.getAbsolutePath().lastIndexOf(File.separator));
		Path pathOutputdirectory = Paths.get(pathOfDirectory);

		boolean outputfilePath = Files.exists(pathOutputdirectory, new LinkOption[] { LinkOption.NOFOLLOW_LINKS });
		if (outputfilePath == false) {
			errorMessageList.put("pathNotExists",
					"Das Verzeichnis in dem die zu importierende CSV Datei abgelegt sein soll, existiert nicht!");
		}

		// Check existence of csv file and overwrite information
		Path pathImportFile = Paths.get(csvFileName);

		boolean outputfileExists = Files.exists(pathImportFile, new LinkOption[] { LinkOption.NOFOLLOW_LINKS });
		if (!outputfileExists) {
			errorMessageList.put("csvFileNotExist",
					"Die zu importierende CSV Datei existiert nicht. Bitte überprüfen Sie die Pfadangabe in der Konfigurationsdatei!");
		}

		/* At third check Connection Information to database */
		if (dbType != null && (dbType.toLowerCase().equals("mysql") || dbType.toLowerCase().equals("postgresql")
				|| dbType.toLowerCase().equals("mssqlserver") || dbType.toLowerCase().equals("sqlite"))) {

		} else {
			errorMessageList.put("database",
					"Bitte geben Sie eines der folgenden unterstützten Datenbanksystem (mysql, postgresql,mssqlserver, sqlite) an!");
		}

		try {
			if (!InetAddress.getByName(localhost).isReachable(1000)) {
				errorMessageList.put("database",
						"Der von Ihnen angegebene HOST (Server) " + localhost + " ist nicht erreichbar!");
			}
		} catch (IOException ioe) {
			errorMessageList.put("database",
					"Der von Ihnen angegebene HOST (Server) " + localhost + "ist nicht erreichbar!");
		}

		Connection con = null;

		if (dbType.equals("sqlite")) {

			try {
				con = DriverManager.getConnection("jdbc:sqlite://" + database);
				if (con.isValid(1000)) {
//					con.close();

				} else {
					errorMessageList.put("database",
							"Es konnte keine Verbindung mit dem SQLite Datenbanksystem hergestellt werden!");
					errorMessageList.put("database2", "Bitte überprüfen Sie die Verbindungsparameter!!");
				}

			} catch (SQLException ex) {
				// handle any errors
				errorMessageList.put("database_exc0", "Es ist ein Fehler aufgetreten!");
				errorMessageList.put("database_exc1", ex.getMessage());
				errorMessageList.put("database_exc2", ex.getSQLState());
			}
		}

		if (dbType.equals("mysql")) {

			try {
				con = DriverManager.getConnection("jdbc:mysql://" + localhost + ":" + port + "/"
						+ database + "?" + "user=" + user + "&password=" + password
						+ "&rewriteBatchedStatements=true");
				if (con.isValid(1000)) {
//					con.close();

				} else {
					errorMessageList.put("database",
							"Es konnte keine Verbindung mit dem SQLite Datenbanksystem hergestellt werden!");
					errorMessageList.put("database2", "Bitte überprüfen Sie die Verbindungsparameter!!");
				}

			} catch (SQLException ex) {
				errorMessageList.put("database_exc0", "Es ist ein Fehler aufgetreten!");
				errorMessageList.put("database_exc1", ex.getMessage());
				errorMessageList.put("database_exc2", ex.getSQLState());
			}
		}

		boolean tableNameInConfigFile = false;
		if (tableName == null || tableName.trim().equals("")) {
			errorMessageList.put("table", "Bitte geben Sie eine Zieltabelle in der Konfigurationsdatei an!");

		} else {
			tableNameInConfigFile = true;
		}

		boolean tableExists = false;
		try {
			java.sql.DatabaseMetaData dbMD = con.getMetaData();
			if (tableNameInConfigFile == true) {
				/*
				 * Hier muss überprüft werden, ob die Tabelle tableName
				 * exisitert
				 */

				if (con.isValid(5)) {

					ResultSet rsTables = dbMD.getTables(null, null, tableName.toLowerCase(), null);
					while (rsTables.next()) {
						String tableNameValue = rsTables.getString(1);
						tableExists = true;

					}
					rsTables.close();
				}

				if (tableExists == false) {
					errorMessageList.put("table2",
							"Die in der Konfigurationsdatei angegebene Tabelle existiert nicht!");
				}
			}

			for (int i = 0; i < fieldToColumnList.size(); i++) {
				FieldCSVToDb csvToDbColumn = fieldToColumnList.get(i);
				boolean mappingError = false;
				if(!(csvToDbColumn.getCvsField() != null &&  !csvToDbColumn.getCvsField().trim().equals(""))){
					errorMessageList.put(Integer.toString(i),"Mapping Fehler: Ein CSV Spaltenbezeichner wurde nicht definiert!");
					mappingError=true;
					
				}
				
				if(!(csvToDbColumn.getDbField() != null &&  !csvToDbColumn.getDbField().trim().equals(""))){
					errorMessageList.put(Integer.toString(i),"Mapping Fehler: Eine Spalte aus der Datenbank Tabelle wurde nicht definiert!");
					mappingError=true;
				}
				
				if(mappingError==true){
					continue;
				}
					
				ResultSet rsColumns = dbMD.getColumns(null, null, tableName, csvToDbColumn.getDbField());
				boolean columnExists = false;
				if (rsColumns != null) {
					while (rsColumns.next()) {
						columnExists = true;
					}
				}
				else{
					columnExists=false;
				}
				rsColumns.close();
				if(columnExists==false){
					errorMessageList.put(csvToDbColumn.getDbField(),
							"Die Spalte " + csvToDbColumn.getDbField() + " aus der Tabelle " + tableName + " die mit der Spalte " + csvToDbColumn.getCvsField() + " der CSV Datei gematched werden soll existiert nicht in der Zieltabelle!");
				}
			}
		} catch (SQLException e) {
			errorMessageList.put("Datenbankausname", e.getLocalizedMessage());
		}

		return errorMessageList;
	}

}
