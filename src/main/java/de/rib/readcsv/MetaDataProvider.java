package de.rib.readcsv;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class MetaDataProvider {
	public static void addColumnTypeToFieldList(List<FieldCSVToDb> list,String table,Connection con) {
		DatabaseMetaData dBM = null;
		ResultSet rsColumnMeta = null;
		try {
			for (FieldCSVToDb fCTD : list) {
				dBM = con.getMetaData();
				rsColumnMeta = dBM.getColumns(null, null, table, fCTD.getDbField());

				rsColumnMeta.next();
				int typeOfColumn = rsColumnMeta.getInt(5);

				fCTD.setType(typeOfColumn);
			}

			// System.out.println("Verbindung in TypMetaInformation ist Null: "
			// + con==null);
			// System.out.println("Verbindung in geschlossen: " +
			// con.isClosed());
			rsColumnMeta.close();
		} catch (SQLException e) {
			System.out.println("****     " + e.getLocalizedMessage());
		}

	}
}
