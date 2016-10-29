package de.ro.readcsv;

import java.io.FileReader;
import java.io.Reader;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

public class ImportCSVToDb {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			// Datum;Konto1;Konto2;Betrag;RechnungsNr;GutschriftKennz;FaelligkeitDatum;Kostenstelle;Buchungstext;BelegNr;VerdichtungsKz
			Reader in = new FileReader("C:\\Freigabe\\export\\FiBu-Profin-Bewegungsdaten.csv");
			Iterable<CSVRecord> records = CSVFormat.newFormat(';')
					.withHeader("Datum", "Konto1", "Konto2", "Betrag", "RechnungsNr", "GutschriftKennz",
							"FaelligkeitDatum", "Kostenstelle", "Buchungstext", "BelegNr", "VerdichtungsKz")
					.parse(in);
			for (CSVRecord record : records) {
				String datum = record.get("Datum");
				String konto1 = record.get("Konto1");
				String konto2 = record.get("Konto2");
				String betrag = record.get("Betrag");
				String rechnungsNr = record.get("RechnungsNr");
				String gutschriftKennz = record.get("GutschriftKennz");
				String faelligDatum = record.get("FaelligkeitDatum");
				String kostenstelle = record.get("Kostenstelle");
				String buchungstext = record.get("Buchungstext");
				String belegNr = record.get("BelegNr");
				String verdichtKz = record.get("VerdichtungsKz");
				
				System.out.println(datum + ":" + konto1 + ":" + konto2);
				//Ein Kommentar

			}
			
			
			/*
			Reader in = new FileReader("C:\\Freigabe\\export\\FiBu-Profin-Bewegungsdaten.csv");
			Iterable<CSVRecord> records = CSVFormat.newFormat(';').parse(in);
			for (CSVRecord record : records) {
			    String columnOne = record.get(0);
		    String columnTwo = record.get(1);
			    System.out.println(columnOne);
			}
			*/

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
