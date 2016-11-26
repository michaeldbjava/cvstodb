package de.rib.readcsv;

import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import de.rib.datehelper.ConvertDateToIso;

public class ImportCSVToDb {

	public static void main(String[] args) {
		String pathOfConfigFile =args[0];;
		boolean configFileExists=false;
		/* If first argument is an path to config file then use it. In other case use default file*/
		if(args.length!=0){
			Path path = Paths.get(pathOfConfigFile);
			System.out.println("Übergebener Pfad: " + pathOfConfigFile);
			configFileExists= Files.exists(path,new LinkOption[]{ LinkOption.NOFOLLOW_LINKS});
			System.out.println("Config File Exists: " + configFileExists);
		}
		// Als erstes wird ein Objekt vom Typ ConfigurationCvsToDB erstellt.
		ConfigurationCvsToDB cvsDBConfig = new ConfigurationCvsToDB();
		if(configFileExists==true){
			System.out.println("Lese Config Datei: "+pathOfConfigFile);
			cvsDBConfig.readConfigFile(pathOfConfigFile);
		}
		else{
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

			ArrayList<FieldCVSToDb> listOfFields = cvsDBConfig.getMapList();

			// Datum;Konto1;Konto2;Betrag;RechnungsNr;GutschriftKennz;FaelligkeitDatum;Kostenstelle;Buchungstext;BelegNr;VerdichtungsKz
			//Reader in = new FileReader(cvsDBConfig.getCvsfile());
			File file = new File(cvsDBConfig.getCvsfile());
			/*
			 * Iterable<CSVRecord> records = CSVFormat.newFormat(';')
			 * .withHeader("Datum", "Konto1", "Konto2", "Betrag", "RechnungsNr",
			 * "GutschriftKennz", "FaelligkeitDatum", "Kostenstelle",
			 * "Buchungstext", "BelegNr", "VerdichtungsKz") .parse(in);
			 */
			CSVFormat csvFormat = CSVFormat.newFormat(cvsDBConfig.getDelimeter()).withRecordSeparator("\n").withFirstRecordAsHeader();
			//Iterable<CSVRecord> records = CSVFormat.newFormat(cvsDBConfig.getDelimeter()).withFirstRecordAsHeader().parse(in);
			
			 CSVParser parser = CSVParser.parse(file,StandardCharsets.UTF_8, csvFormat);
			 Iterable<CSVRecord> records = parser.getRecords();
			String columnList = "(";
			for (int i = 0; i < listOfFields.size(); i++) {
				FieldCVSToDb fCvsDb = listOfFields.get(i);
				if (i == 0) {
					columnList = columnList + fCvsDb.getDbField();
				} else {
					columnList = columnList + "," + fCvsDb.getDbField();
				}

			}
			columnList = columnList + ")";
			// System.out.println("Column List: " + columnList);

			con.setAutoCommit(false);
			Statement statement = con.createStatement();
			TypMetaInformation tMI = new TypMetaInformation();
			for (CSVRecord record : records) {
				String valueList = "(";
				for (int i = 0; i < listOfFields.size(); i++) {
					FieldCVSToDb fCvsDb = listOfFields.get(i);
					boolean isNumericType = tMI.isNumeric(cvsDBConfig.getTable(), fCvsDb.getDbField(), con);
					boolean isDateType = tMI.isDate(cvsDBConfig.getTable(), fCvsDb.getDbField(), con);
					String value = record.get(fCvsDb.getCvsField());
					if (isNumericType == true) {
						if (i == 0) {
							valueList = valueList + value.replace(',', '.');
						} else {
							valueList = valueList + "," + value.replace(',', '.');

						}
					} else {
						if (isDateType == true) {
							if (cvsDBConfig.isDateIsISODate()) {
								if (i == 0) {
									valueList = valueList + "'" + ConvertDateToIso.convert(value) + "'";
								} else {
									valueList = valueList + ",'" + ConvertDateToIso.convert(value) + "'";

								}
							} else {
								if (i == 0) {
									valueList = valueList + "'" +value  + "'";
								} else {
									valueList = valueList + ",'" + value + "'";

								}
							}
						} else {
							if (i == 0) {
								valueList = valueList + "'" + value + "'";
							} else {
								valueList = valueList + ",'" + value + "'";

							}

						}

					}

				}
				valueList = valueList + ")";
				System.out.println("INSERT INTO " + cvsDBConfig.getTable() + " " + columnList + " VALUES " + valueList);
				statement.addBatch("INSERT INTO " + cvsDBConfig.getTable() + " " + columnList + " VALUES " + valueList);
			}
			statement.executeBatch();
			con.commit();
			
			statement.close();
			/*
			 * Reader in = new
			 * FileReader("C:\\Freigabe\\export\\FiBu-Profin-Bewegungsdaten.csv"
			 * ); Iterable<CSVRecord> records =
			 * CSVFormat.newFormat(';').parse(in); for (CSVRecord record :
			 * records) { String columnOne = record.get(0); String columnTwo =
			 * record.get(1); System.out.println(columnOne); }
			 */

		} catch (Exception e) {
			e.printStackTrace();
			try {
				con.rollback();
			} catch (Exception e1) {

			}
		}

	}

}
