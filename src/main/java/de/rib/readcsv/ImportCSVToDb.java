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
		String pathOfConfigFile = null;
		if (args.length == 1) {
			pathOfConfigFile = args[0];
		}
		boolean configFileExists = false;
		/*
		 * If first argument is an path to config file then use it. In other
		 * case use default file
		 */
		System.out.print("\033[H\033[2J");
		System.out.flush();
		if (pathOfConfigFile != null) {
			Path path = Paths.get(pathOfConfigFile);
			// System.out.println("�bergebener Pfad: " + pathOfConfigFile);
			configFileExists = Files.exists(path, new LinkOption[] { LinkOption.NOFOLLOW_LINKS });

			// Als erstes wird ein Objekt vom Typ ConfigurationCvsToDB erstellt.

			if (configFileExists == true) {
				ConfigurationCsvToDB cvsDBConfig = new ConfigurationCsvToDB();
				System.out.println("********************************************");
				System.out.println("********************************************");
				System.out.println("****                                  ******");
				System.out.println("****    Starte Import CSV nach DB!    ******");
				System.out.println("****                                  ******");
				System.out.println("********************************************");
				System.out.println("********************************************");
				System.out.println("****    ");
				System.out.println("****    1. Lese Config Datei: " + pathOfConfigFile);
				System.out.println("****    ");
				cvsDBConfig.readConfigFile(pathOfConfigFile);
				Connection con = cvsDBConfig.getConnectionToDb();
				// TODO Auto-generated method stub
				try {
					// Example CVS File must be places in project dircetory

					/*
					 * Als erstes Konfigurationsdatei auslesen (mit einem
					 * kleinen DOM Parser
					 */

					ArrayList<FieldCSVToDb> listOfFields = cvsDBConfig.getMapList();

					File file = new File(cvsDBConfig.getCvsfile());
					System.out.println("****     2. Lese die Zeilen aus der CSV Datei " + file.getName() + " aus!");
					System.out.println("****     ");
					System.out.println(
							"****     3. Als Delimeter (Spaltentrennzeichen) wird folgendes Zeichen verwendet ["
									+ cvsDBConfig.getDelimeter() + "]");
					System.out.println("****     ");
					CSVFormat csvFormat = CSVFormat.newFormat(cvsDBConfig.getDelimeter()).withRecordSeparator("\n")
							.withFirstRecordAsHeader();

					CSVParser parser = CSVParser.parse(file, StandardCharsets.UTF_8, csvFormat);
					Iterable<CSVRecord> records = parser.getRecords();
					String columnList = "(";
					String columnUpdateList = " ";
					DatabaseMetaData dmD = con.getMetaData();
					ResultSet rsPrimaryKey = dmD.getPrimaryKeys(null, null, cvsDBConfig.getTable());
					List<String> primaryKeyList = new ArrayList<String>();
					while (rsPrimaryKey.next()) {
						String columnNamePK = rsPrimaryKey.getString(4);
						primaryKeyList.add(columnNamePK);
					}

					/* Erzeuge eine Spalten Liste, f�r die Insert Anweisung */
					for (int i = 0; i < listOfFields.size(); i++) {
						FieldCSVToDb fCvsDb = listOfFields.get(i);
						if (i == 0) {
							columnList = columnList + fCvsDb.getDbField();

						} else {
							columnList = columnList + "," + fCvsDb.getDbField();

						}

					}

					/*
					 * Liste definieren, die ausschlie�lich Felder aufnimmt, die
					 * aktualisiert werden sollen. Diese Liste darf keine
					 * Prim�rschl�sselspalten enthalten
					 */
					List<FieldCSVToDb> updateFields = new ArrayList<FieldCSVToDb>();
					for (int i = 0; i < listOfFields.size(); i++) {
						FieldCSVToDb fCvsDb = listOfFields.get(i);
						if (!primaryKeyList.contains(fCvsDb.getDbField())) {
							updateFields.add(fCvsDb);
						}
					}

					/*
					 * Zusammensetzen der Wertzuweisungen (=?) der Update
					 * Spalten, f�r das PreparedStatement
					 */
					for (int i = 0; i < updateFields.size(); i++) {
						FieldCSVToDb fCvsDb2 = updateFields.get(i);
						if (i == 0) {
							columnUpdateList = columnUpdateList + fCvsDb2.getDbField() + "=?";
						} else {
							columnUpdateList = columnUpdateList + "," + fCvsDb2.getDbField() + "=?";
						}
					}

					// columnUpdateList=columnUpdateList.replaceFirst(",", "");
					columnList = columnList + ")";
					// System.out.println(columnUpdateList);

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
					 * Hier werden die Feld Ausdr�cke, die in der
					 * Konfigurationsdatei angegeben werden, zusammengebaut. Da
					 * ich hier jetzt Prepared Statements verwende, m�ssen diese
					 * zun�chst mit in die ? Stellvertreterlist f�r Werte mit
					 * aufgenommen werden.
					 * 
					 * Das Gleiche gilt nat�rlich f�r die Komma separierte Liste
					 * der Spaltennamen.
					 * 
					 * 04.03.2017 Beginne damit, die Ausdr�cke einzubauen
					 */

					/*
					 * 
					 * DAs hier muss noch eingebaut werden, aber zuerst das andere fertig machen ....
					  ArrayList<FieldEXPRESSIONToDB> listExpressions = cvsDBConfig.getMapListExpressions();
					 

					if (listExpressions != null) {
						for (int i = 0; i < listExpressions.size(); i++) {
							FieldEXPRESSIONToDB fETDB = listExpressions.get(i);
							if (i == 0) {
								columnList = columnList + "," + fETDB.getTableColumn();
								placeholderValues = placeholderValues + ",?";
							} else {
								columnList = columnList + "," + fETDB.getTableColumn();
								placeholderValues = placeholderValues + ",?";
							}

						}
					}*/

					String sqlInstert = "INSERT INTO " + cvsDBConfig.getTable() + " " + columnList + " VALUES ("
							+ placeholderValues + ") ON DUPLICATE KEY UPDATE " + columnUpdateList;
					System.out.println("****     4.0 PREPARED STATEMENT wurde formuliert!");
					System.out.println("****    ");
					java.sql.PreparedStatement prepStatement = con.prepareStatement(sqlInstert);

					// System.out.println("Column List: " + columnList);

					// System.out.println(columnList);

					con.setAutoCommit(false);
					for (CSVRecord record : records) {

						int j = 0;
						for (int i = 0; i < listOfFields.size(); i++) {
							j++;

							/*
							 * if(i==13) System.out.println("i ist gleich 13");
							 */
							FieldCSVToDb fCvsDb = listOfFields.get(i);

							int typOfColumn = fCvsDb.getType();

							String value = record.get(fCvsDb.getCvsField());
							/*
							 * System.out.println("Wert von j: " + j +
							 * " Wert aus Zeile:" + value );
							 */

							switch (typOfColumn) {
							case Types.ARRAY:
								// System.out.println("Array Typ is not
								// supported");
								break;
							case Types.BIGINT:
								prepStatement.setInt(j, Integer.parseInt(value));
								break;
							case Types.BINARY:
								// System.out.println("Binary Typ is not
								// supported");
								break;
							case Types.BIT:
								prepStatement.setBoolean(j, Boolean.parseBoolean(value));
								break;
							case Types.BLOB:
								// System.out.println("Blob Typ is not
								// supported");
								break;
							case Types.BOOLEAN:
								prepStatement.setBoolean(j, Boolean.parseBoolean(value));
								break;
							case Types.CHAR:
								prepStatement.setString(j, value);
								break;
							case Types.CLOB:
								// System.out.println("CLOB Typ is not
								// supported");
								break;
							case Types.DATALINK:
								// System.out.println("DATALINK Typ is not
								// supported");
								break;
							case Types.DATE:
								String pattern = "yyyy-MM-dd";
								SimpleDateFormat formatOfDate = new SimpleDateFormat(pattern);
								prepStatement.setDate(j, new java.sql.Date(formatOfDate.parse(value).getTime()));
								break;
							case Types.DECIMAL:
								prepStatement.setBigDecimal(j,
										BigDecimal.valueOf(Double.parseDouble(value.replace(',', '.'))));
								break;
							case Types.DISTINCT:
								// System.out.println("DISTINCT Typ is not
								// supported");
								break;
							case Types.DOUBLE:
								prepStatement.setDouble(j, Double.parseDouble(value));
								break;
							case Types.FLOAT:
								prepStatement.setFloat(j, Float.parseFloat(value));
								break;
							case Types.INTEGER:
								prepStatement.setInt(j, Integer.parseInt(value));
								break;
							case Types.JAVA_OBJECT:
								// System.out.println("JAVA_OBJECT Typ is not
								// supported");
								break;
							case Types.LONGNVARCHAR:
								prepStatement.setString(j, value);
								break;
							case Types.LONGVARBINARY:
								prepStatement.setString(j, value);
								break;
							case Types.LONGVARCHAR:
								prepStatement.setString(j, value);
								break;
							case Types.NCHAR:
								prepStatement.setString(j, value);
								break;
							case Types.NCLOB:
								prepStatement.setString(j, value);
								break;
							case Types.NULL:
								// System.out.println("NULL Typ is not
								// supported");
								;
								break;
							case Types.NUMERIC:
								prepStatement.setDouble(j, Double.parseDouble(value));
								break;
							case Types.NVARCHAR:
								prepStatement.setString(j, value);
								break;
							case Types.OTHER:
								// System.out.println("OTHER Typ is not
								// supported");
								break;
							case Types.REAL:
								System.out.println("REAL Typ is not supported");
								break;
							case Types.REF:
								// System.out.println("REF Typ is not
								// supported");
								break;
							case Types.REF_CURSOR:
								// System.out.println("REF_CURSOR Typ is not
								// supported");
								break;
							case Types.ROWID:
								// System.out.println("ROWID Typ is not
								// supported");
								break;
							case Types.SMALLINT:
								prepStatement.setInt(j, Integer.parseInt(value));
								break;
							case Types.SQLXML:
								// System.out.println("SQLXML Typ is not
								// supported");
								break;
							case Types.STRUCT:
								// System.out.println("STRUCT Typ is not
								// supported");
								break;
							case Types.TIME:
								prepStatement.setTime(j, new java.sql.Time(Integer.parseInt(value)));
								break;
							case Types.TIME_WITH_TIMEZONE:
								prepStatement.setTime(j, new java.sql.Time(Integer.parseInt(value)));
								break;
							case Types.TIMESTAMP:
								SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
								prepStatement.setTimestamp(j, new Timestamp(dateFormat.parse(value).getTime()));
								break;
							case Types.TIMESTAMP_WITH_TIMEZONE:
								prepStatement.setTimestamp(j, new java.sql.Timestamp(Long.parseLong(value)));
								break;
							case Types.TINYINT:
								prepStatement.setInt(j, Integer.parseInt(value));
								break;
							case Types.VARBINARY:
								// System.out.println("VARBINARY Typ is not
								// supported");
								break;
							case Types.VARCHAR:
								prepStatement.setString(j, value);
								break;

							}
						}

						/*
						 * Das m�ssen wir uns noch mal gut Ausdr�cke, die in der
						 * XML Datei angegeben werden, lassen sich
						 * wahrscheinlich nicht in PreparedStatements verwenden.
						 * Insbesondere dann, wenn wir hier Funktionsaufrufe
						 * festlegen. for(int i=0;i<listExpressions.size();i++){
						 * j++; FieldEXPRESSIONToDB fEXToDb =
						 * listExpressions.get(i);
						 * 
						 * }
						 */

						/*
						 * Ich muss mir die Felder, die �brigbleiben holen und
						 * in der Reihenfolge einf�gen, wie Sie in den
						 * Wertzuweisungen vorliegen Ich muss zun�chst die
						 * Spalten ermitteln, die nicht Prim�schl�sselspalten
						 * sind.
						 */
						for (int i = 0; i < updateFields.size(); i++) {
							j++;
							FieldCSVToDb fCvsDb = updateFields.get(i);
							int typOfColumn = fCvsDb.getType();

							String value = record.get(fCvsDb.getCvsField());
							/*
							 * System.out.println("Wert von j: " + j +
							 * " Wert aus Zeile:" + value );
							 */

							switch (typOfColumn) {
							case Types.ARRAY:
								// System.out.println("Array Typ is not
								// supported");
								break;
							case Types.BIGINT:
								prepStatement.setInt(j, Integer.parseInt(value));
								break;
							case Types.BINARY:
								// System.out.println("Binary Typ is not
								// supported");
								break;
							case Types.BIT:
								prepStatement.setBoolean(j, Boolean.parseBoolean(value));
								break;
							case Types.BLOB:
								// System.out.println("Blob Typ is not
								// supported");
								break;
							case Types.BOOLEAN:
								prepStatement.setBoolean(j, Boolean.parseBoolean(value));
								break;
							case Types.CHAR:
								prepStatement.setString(j, value);
								break;
							case Types.CLOB:
								// System.out.println("CLOB Typ is not
								// supported");
								break;
							case Types.DATALINK:
								// System.out.println("DATALINK Typ is not
								// supported");
								break;
							case Types.DATE:
								String pattern = "yyyy-MM-dd";
								SimpleDateFormat formatOfDate = new SimpleDateFormat(pattern);
								prepStatement.setDate(j, new java.sql.Date(formatOfDate.parse(value).getTime()));
								break;
							case Types.DECIMAL:
								prepStatement.setBigDecimal(j,
										BigDecimal.valueOf(Double.parseDouble(value.replace(',', '.'))));
								break;
							case Types.DISTINCT:
								// System.out.println("DISTINCT Typ is not
								// supported");
								break;
							case Types.DOUBLE:
								prepStatement.setDouble(j, Double.parseDouble(value));
								break;
							case Types.FLOAT:
								prepStatement.setFloat(j, Float.parseFloat(value));
								break;
							case Types.INTEGER:
								prepStatement.setInt(j, Integer.parseInt(value));
								break;
							case Types.JAVA_OBJECT:
								// System.out.println("JAVA_OBJECT Typ is not
								// supported");
								break;
							case Types.LONGNVARCHAR:
								prepStatement.setString(j, value);
								break;
							case Types.LONGVARBINARY:
								prepStatement.setString(j, value);
								break;
							case Types.LONGVARCHAR:
								prepStatement.setString(j, value);
								break;
							case Types.NCHAR:
								prepStatement.setString(j, value);
								break;
							case Types.NCLOB:
								prepStatement.setString(j, value);
								break;
							case Types.NULL:
								// System.out.println("NULL Typ is not
								// supported");
								;
								break;
							case Types.NUMERIC:
								prepStatement.setDouble(j, Double.parseDouble(value));
								break;
							case Types.NVARCHAR:
								prepStatement.setString(j, value);
								break;
							case Types.OTHER:
								// System.out.println("OTHER Typ is not
								// supported");
								break;
							case Types.REAL:
								// System.out.println("REAL Typ is not
								// supported");
								break;
							case Types.REF:
								// System.out.println("REF Typ is not
								// supported");
								break;
							case Types.REF_CURSOR:
								// System.out.println("REF_CURSOR Typ is not
								// supported");
								break;
							case Types.ROWID:
								// System.out.println("ROWID Typ is not
								// supported");
								break;
							case Types.SMALLINT:
								prepStatement.setInt(j, Integer.parseInt(value));
								break;
							case Types.SQLXML:
								// System.out.println("SQLXML Typ is not
								// supported");
								break;
							case Types.STRUCT:
								// System.out.println("STRUCT Typ is not
								// supported");
								break;
							case Types.TIME:
								prepStatement.setTime(j, new java.sql.Time(Integer.parseInt(value)));
								break;
							case Types.TIME_WITH_TIMEZONE:
								prepStatement.setTime(j, new java.sql.Time(Integer.parseInt(value)));
								break;
							case Types.TIMESTAMP:
								SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
								prepStatement.setTimestamp(j, new Timestamp(dateFormat.parse(value).getTime()));
								break;
							case Types.TIMESTAMP_WITH_TIMEZONE:
								prepStatement.setTimestamp(j, new java.sql.Timestamp(Long.parseLong(value)));
								break;
							case Types.TINYINT:
								prepStatement.setInt(j, Integer.parseInt(value));
								break;
							case Types.VARBINARY:
								// System.out.println("VARBINARY Typ is not
								// supported");
								break;
							case Types.VARCHAR:
								prepStatement.setString(j, value);
								break;

							}

						}

						/* 04.03.2017 Erg�nzung muss noch vorgenommen werden */
						/*
						if (listExpressions != null) {

							for (int i = 0; i < listExpressions.size(); i++) {
								FieldEXPRESSIONToDB fETDB = listExpressions.get(i);
								
								int type = fETDB.getColumnTyp();
								String value=fETDB.getExpression();
								switch (type) {
								case Types.ARRAY:
									// System.out.println("Array Typ is not
									// supported");
									break;
								case Types.BIGINT:
									prepStatement.setInt(j, Integer.parseInt(value));
									break;
								case Types.BINARY:
									// System.out.println("Binary Typ is not
									// supported");
									break;
								case Types.BIT:
									prepStatement.setBoolean(j, Boolean.parseBoolean(value));
									break;
								case Types.BLOB:
									// System.out.println("Blob Typ is not
									// supported");
									break;
								case Types.BOOLEAN:
									prepStatement.setBoolean(j, Boolean.parseBoolean(value));
									break;
								case Types.CHAR:
									prepStatement.setString(j, value);
									break;
								case Types.CLOB:
									// System.out.println("CLOB Typ is not
									// supported");
									break;
								case Types.DATALINK:
									// System.out.println("DATALINK Typ is not
									// supported");
									break;
								case Types.DATE:
									String pattern = "yyyy-MM-dd";
									SimpleDateFormat formatOfDate = new SimpleDateFormat(pattern);
									prepStatement.setDate(j, new java.sql.Date(formatOfDate.parse(value).getTime()));
									break;
								case Types.DECIMAL:
									prepStatement.setBigDecimal(j,
											BigDecimal.valueOf(Double.parseDouble(value.replace(',', '.'))));
									break;
								case Types.DISTINCT:
									// System.out.println("DISTINCT Typ is not
									// supported");
									break;
								case Types.DOUBLE:
									prepStatement.setDouble(j, Double.parseDouble(value));
									break;
								case Types.FLOAT:
									prepStatement.setFloat(j, Float.parseFloat(value));
									break;
								case Types.INTEGER:
									prepStatement.setInt(j, Integer.parseInt(value));
									break;
								case Types.JAVA_OBJECT:
									// System.out.println("JAVA_OBJECT Typ is not
									// supported");
									break;
								case Types.LONGNVARCHAR:
									prepStatement.setString(j, value);
									break;
								case Types.LONGVARBINARY:
									prepStatement.setString(j, value);
									break;
								case Types.LONGVARCHAR:
									prepStatement.setString(j, value);
									break;
								case Types.NCHAR:
									prepStatement.setString(j, value);
									break;
								case Types.NCLOB:
									prepStatement.setString(j, value);
									break;
								case Types.NULL:
									// System.out.println("NULL Typ is not
									// supported");
									;
									break;
								case Types.NUMERIC:
									prepStatement.setDouble(j, Double.parseDouble(value));
									break;
								case Types.NVARCHAR:
									prepStatement.setString(j, value);
									break;
								case Types.OTHER:
									// System.out.println("OTHER Typ is not
									// supported");
									break;
								case Types.REAL:
									// System.out.println("REAL Typ is not
									// supported");
									break;
								case Types.REF:
									// System.out.println("REF Typ is not
									// supported");
									break;
								case Types.REF_CURSOR:
									// System.out.println("REF_CURSOR Typ is not
									// supported");
									break;
								case Types.ROWID:
									// System.out.println("ROWID Typ is not
									// supported");
									break;
								case Types.SMALLINT:
									prepStatement.setInt(j, Integer.parseInt(value));
									break;
								case Types.SQLXML:
									// System.out.println("SQLXML Typ is not
									// supported");
									break;
								case Types.STRUCT:
									// System.out.println("STRUCT Typ is not
									// supported");
									break;
								case Types.TIME:
									prepStatement.setTime(j, new java.sql.Time(Integer.parseInt(value)));
									break;
								case Types.TIME_WITH_TIMEZONE:
									prepStatement.setTime(j, new java.sql.Time(Integer.parseInt(value)));
									break;
								case Types.TIMESTAMP:
									SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
									prepStatement.setTimestamp(j, new Timestamp(dateFormat.parse(value).getTime()));
									break;
								case Types.TIMESTAMP_WITH_TIMEZONE:
									prepStatement.setTimestamp(j, new java.sql.Timestamp(Long.parseLong(value)));
									break;
								case Types.TINYINT:
									prepStatement.setInt(j, Integer.parseInt(value));
									break;
								case Types.VARBINARY:
									// System.out.println("VARBINARY Typ is not
									// supported");
									break;
								case Types.VARCHAR:
									prepStatement.setString(j, value);
									break;

								}

							}
						}*/
						prepStatement.addBatch();

					}

					prepStatement.executeBatch();

					System.out.println("****     ");

					con.commit();

					con.close();
					System.out.println(
							"****     5. Der Import der CSV Datei in die Tabelle wurde erfolgreich ausgef�hrt!");
					System.out.println("****     ");

				} catch (Exception e) {
					System.out.println("****     2. Es ist folgender Fehler aufgetreten: " + e.getLocalizedMessage());
					System.out.println("****     ");

					try {
						System.out.println("****     3. Fuehre einen Rollback durch!");
						System.out.println("****     ");

						con.rollback();
						System.out.println("****     4. Rollback wurde erfolgreich durchgefuehrt!");
						System.out.println("****     ");
					} catch (Exception e1) {

					}
				}
			} else {
				System.out.println(
						"****    1) Die angegebene Konfigurationsdatei ist nicht vorhanden.\n****       Bitte geben Sie eine bestehende Konfigurationsdatei an!"
								+ "\n****       Uebergeben Sie bitte den Pfad zur Konfigurationsdatei als Parameter!"
								+ "\n****\n****       Der Aufruf des Programms muss wie folgt erfolgen: "
								+ "\n****\n****        " + "\n****       java -jar csvtodb.jar csvtodb_config_xxx.xml"
								+ "\n****\n****        " + "\n****       Lesen Sie bitte die beiligende Dokumentation!"
								+ "\n****\n****        ");

				System.out.println("****    2) Das Import Programm wird abgebrochen!" + "\n****\n****       ");
			}
		} else {
			System.out.println(
					"****    1) Es wurde keine Konfigurationsdatei angegeben.\n****       Bitte geben Sie eine Konfigurationsdatei an!"
							+ "\n****       Uebergeben Sie bitte den Pfad zur Konfigurationsdatei als Parameter!"
							+ "\n****\n****       Der Aufruf des Programms muss wie folgt erfolgen: "
							+ "\n****\n****        " + "\n****       java -jar csvtodb.jar csvtodb_config_xxx.xml"
							+ "\n****\n****        " + "\n****       Lesen Sie bitte die beiligende Dokumentation!"
							+ "\n****\n****        ");

			System.out.println("****    2) Das Import Programm wird abgebrochen!" + "\n****\n****       ");

		}
		System.out.println("********************************************");
		System.out.println("********************************************");
		System.out.println("****                                  ******");
		System.out.println("****    Beende Import CSV nach DB!    ******");
		System.out.println("****                                  ******");
		System.out.println("********************************************");
		System.out.println("********************************************");

	}

}
