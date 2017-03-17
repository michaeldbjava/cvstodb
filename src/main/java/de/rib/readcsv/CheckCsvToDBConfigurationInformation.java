package de.rib.readcsv;

import java.util.HashMap;
import java.util.Map;


public class CheckCsvToDBConfigurationInformation {
private ConfigurationCsvToDB configCsvToDb;
	public CheckCsvToDBConfigurationInformation(ConfigurationCsvToDB configCsvToDb) {
		// TODO Auto-generated constructor stub
		this.configCsvToDb=configCsvToDb;
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
		
		
		return errorMessageList;
	}

}
