package de.rib.readcsv;

import java.io.FileReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import de.rib.datehelper.ConvertDateToIso;

public class ImportCSVToDb {

	public static void main(String[] args) {
		// Als erstes wird ein Objekt vom Typ ConfigurationCvsToDB erstellt.
		ConfigurationCvsToDB cvsDBConfig = new ConfigurationCvsToDB();
		cvsDBConfig.readConfigFile("csvtodb_config.xml");
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
			Reader in = new FileReader(cvsDBConfig.getCvsfile());
			/*
			 * Iterable<CSVRecord> records = CSVFormat.newFormat(';')
			 * .withHeader("Datum", "Konto1", "Konto2", "Betrag", "RechnungsNr",
			 * "GutschriftKennz", "FaelligkeitDatum", "Kostenstelle",
			 * "Buchungstext", "BelegNr", "VerdichtungsKz") .parse(in);
			 */

			Iterable<CSVRecord> records = CSVFormat.newFormat(cvsDBConfig.getDelimeter()).withFirstRecordAsHeader().parse(in);
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
//			System.out.println("Column List: " + columnList);
			
			con.setAutoCommit(false);
			Statement statement = con.createStatement();
			TypMetaInformation tMI= new TypMetaInformation();
			for (CSVRecord record : records) {
				String valueList = "(";
				for (int i = 0; i < listOfFields.size(); i++) {
					FieldCVSToDb fCvsDb = listOfFields.get(i);
					boolean isNumericType = tMI.isNumeric(cvsDBConfig.getTable(),
							fCvsDb.getDbField(), con);
					boolean isDateType = tMI.isDate(cvsDBConfig.getTable(),
							fCvsDb.getDbField(), con);
					String value = record.get(fCvsDb.getCvsField());
					if (isNumericType == true) {
						if (i == 0) {
							valueList = valueList + value.replace(',', '.');
						} else {
							valueList = valueList + "," + value.replace(',', '.');

						}
					} else {
						if(isDateType==true){
							if (i == 0) {
								valueList = valueList +  "'" + ConvertDateToIso.convert(value) + "'";
							} else {
								valueList = valueList + ",'" + ConvertDateToIso.convert(value) + "'";

							}
						}
						else{
							if (i == 0) {
								valueList = valueList +  "'" + value + "'";
							} else {
								valueList = valueList + ",'" + value + "'";

							}

						}
						
					}

				}
				valueList = valueList + ")";
				System.out.println("INSERT INTO " + cvsDBConfig.getTable() +" "+ columnList + " VALUES " + valueList);
				statement.addBatch("INSERT INTO " + cvsDBConfig.getTable() +" "+ columnList + " VALUES " + valueList);
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
			try{
			con.rollback();
			}
			catch(Exception e1){
				
			}
		}

	}

}
