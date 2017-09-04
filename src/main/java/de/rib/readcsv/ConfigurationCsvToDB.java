package de.rib.readcsv;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import de.rib.errorhandling.CsvToDbErrorHandlingConfigFile;

public class ConfigurationCsvToDB {

	private String cvsfile;
	private String localhost;
	private String database;
	private String dbtype;
	private String port;
	private String user;
	private String password;
	private String table;
	private char delimeter;
	private boolean dateIsISODate = false;
	private ArrayList<FieldCSVToDb> mapList = new ArrayList<FieldCSVToDb>();
	private ArrayList<FieldEXPRESSIONToDB> mapListExpressions = new ArrayList<FieldEXPRESSIONToDB>();
	private Connection conToDb = null;
	private Document document = null;
	
	private int countPoints = 0;

	public String getCvsfile() {
		return cvsfile;
	}

	public void setCvsfile(String cvsfile) {
		this.cvsfile = cvsfile;
	}

	public String getLocalhost() {
		return localhost;
	}

	public void setLocalhost(String localhost) {
		this.localhost = localhost;
	}

	public String getDatabase() {
		return database;
	}

	public void setDatabase(String database) {
		this.database = database;
	}

	public String getPort() {
		return port;
	}

	public void setPort(String port) {
		this.port = port;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public ArrayList<FieldCSVToDb> getMapList() {
		return mapList;
	}

	public void setMapList(ArrayList<FieldCSVToDb> mapList) {
		this.mapList = mapList;
	}

	public ArrayList<FieldEXPRESSIONToDB> getMapListExpressions() {
		return mapListExpressions;
	}

	public void setMapListExpressions(ArrayList<FieldEXPRESSIONToDB> mapListExpressions) {
		this.mapListExpressions = mapListExpressions;
	}

	public boolean addMapField(FieldCSVToDb e) {
		return mapList.add(e);
	}

	public FieldCSVToDb getMapField(int index) {
		return mapList.get(index);
	}

	public String getDbtype() {
		return dbtype;
	}

	public void setDbtype(String dbtype) {
		this.dbtype = dbtype;
	}

	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
	}

	public char getDelimeter() {
		return delimeter;
	}

	public void setDelimeter(char delimeter) {
		this.delimeter = delimeter;
	}

	public boolean isDateIsISODate() {
		return dateIsISODate;
	}

	public void setDateIsISODate(boolean dateIsISODate) {
		this.dateIsISODate = dateIsISODate;
	}

	public boolean readConfigFile(String pathXMLConfigFile) {
		try {
			File xmlFile = new File(pathXMLConfigFile);
			File xsdFile = new File("csvtodb_config_schema.xsd");

			SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
			Schema schema = schemaFactory.newSchema(xsdFile);

			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			dbFactory.setSchema(schema);
			dbFactory.setValidating(false);
			DocumentBuilder documentBuilder = dbFactory.newDocumentBuilder();
			documentBuilder.setErrorHandler(new CsvToDbErrorHandlingConfigFile());
			document = documentBuilder.parse(xmlFile);

			
			
			
			this.setCvsfile(getValueOfXMLNode("csvfile"));

			/* Lese Element delimeter */
			this.setDelimeter(getValueOfXMLNode("delimeter").charAt(0));

			/* Lese Element dbtype */
			this.setDbtype(getValueOfXMLNode("dbtype"));

			/* Lese Element localhost */
			this.setLocalhost(getValueOfXMLNode("host"));

			/* Read DB Name */
			this.setDatabase(getValueOfXMLNode("database_name"));

			/* Read Username */
			this.setUser(getValueOfXMLNode("user"));

			/* Read Password */
			this.setPassword(getValueOfXMLNode("password"));

			/* Read DB Port */
			this.setPort(getValueOfXMLNode("port"));

			/* Read Table */

			this.setTable(getValueOfXMLNode("table"));

			/* Read cvs to db column mappings */
			// map-csv-fields-to-db
			NodeList nodeMapCsvToDb = document.getElementsByTagName("map-csv-fields-to-db");
			Element elementMapCsvToDb = (Element) nodeMapCsvToDb.item(0);
			NodeList nodeListMappingFields = elementMapCsvToDb.getElementsByTagName("field-mapping");
			// NodeList nodeListMappingExpression =
			// elementMapCsvToDb.getElementsByTagName("calculated-value-mapping");
			int j = nodeListMappingFields.getLength();

			// conToDb=this.getConnectionToDb();
			for (int i = 0; i < j; i++) {

				Element elementFieldMapping = (Element) nodeListMappingFields.item(i);

				NodeList nodeCsvField = elementFieldMapping.getElementsByTagName("csv-field");
				NodeList nodeDbField = elementFieldMapping.getElementsByTagName("table-column");
				FieldCSVToDb fCvsToDb = new FieldCSVToDb(nodeCsvField.item(0).getFirstChild().getTextContent(),
						nodeDbField.item(0).getFirstChild().getTextContent());

				/*
				 * try { System.out.
				 * println("Verbindung zur Datenbank ist geschlossen: " +
				 * conToDb.isClosed()); } catch (SQLException e) {
				 * e.printStackTrace(); }
				 */
				// System.out.println(nodeCsvField.item(0).getFirstChild().getTextContent()
				// + "-->"
				// + nodeDbField.item(0).getFirstChild().getTextContent());

				this.addMapField(fCvsToDb);
			}
			// System.out.println("Ermittle die Datentypen zu den Mapping
			// Feldern!");
//			Connection con = ConnectionFactory.createConnectionToDb(cDbToCvs.getDbtype(), cDbToCvs.getLocalhost(), cDbToCvs.getPort(), cDbToCvs.getDatabase(), cDbToCvs.getUser(), cDbToCvs.getPassword());
			conToDb=ConnectionFactory.createConnectionToDb(this.dbtype, this.localhost, this.port, this.database, this.user,this.password);
			MetaDataProvider.addColumnTypeToFieldList(mapList, table, conToDb);
			/*
			 * <map-calculated-fields-to-db> <calculated-value-mapping>
			 * <expression>now()</expression>
			 * <table-column>imported-datum</table-column>
			 * </calculated-value-mapping> </map
			 */
			NodeList nodeListMappingExpressionsFields = document.getElementsByTagName("map-calculated-fields-to-db");
			if (nodeListMappingExpressionsFields != null && nodeListMappingExpressionsFields.getLength()!=0) {
				Element elementMapCalculatedFieldsToDb = (Element) nodeListMappingExpressionsFields.item(0);
				NodeList nodeListMappingCalculatedValuesToFields = elementMapCalculatedFieldsToDb
						.getElementsByTagName("calculated-value-mapping");
				int z = nodeListMappingCalculatedValuesToFields.getLength();
				for (int i = 0; i < z; i++) {
					Element calculatedValueToFieldMapping = (Element) nodeListMappingCalculatedValuesToFields.item(i);
					NodeList nodeExpression = calculatedValueToFieldMapping.getElementsByTagName("expression");
					NodeList nodeTableColumn = calculatedValueToFieldMapping.getElementsByTagName("table-column");
					NodeList nodeTableColumnType = calculatedValueToFieldMapping
							.getElementsByTagName("table-column-type");

					// nodeCsvField.item(0).getFirstChild().getTextContent()
					FieldEXPRESSIONToDB fETDB = new FieldEXPRESSIONToDB(
							nodeExpression.item(0).getFirstChild().getTextContent(),
							nodeTableColumn.item(0).getFirstChild().getTextContent(),
							Integer.parseInt(nodeTableColumnType.item(0).getFirstChild().getTextContent()));
					mapListExpressions.add(fETDB);
				}
			}
			
			} catch (IOException ioe) {
			ioe.printStackTrace();
		} catch (SAXException se) {
			se.printStackTrace();
		} catch (ParserConfigurationException pe) {
			pe.printStackTrace();
		}

		return true;

	}

	
	private String getValueOfXMLNode(String xmlNode) {
		String xmlNodeValue = xmlNode;
		/* Die Methode funktioniert noch nicht */

		NodeList nodeOfTag = document.getElementsByTagName(xmlNodeValue);
		Element elementOfTag = (Element) nodeOfTag.item(0);
		String valueOfElement = elementOfTag.getFirstChild().getTextContent();
		return valueOfElement;
	}

	/*private boolean getTypeOfFilds() {
		DatabaseMetaData dBM = null;
		ResultSet rsColumnMeta = null;
		try {
			for (FieldCSVToDb fCTD : mapList) {
				dBM = conToDb.getMetaData();
				rsColumnMeta = dBM.getColumns(null, null, table, fCTD.getDbField());

				rsColumnMeta.next();
				int typeOfColumn = rsColumnMeta.getInt(5);

				fCTD.setType(typeOfColumn);
			}

			rsColumnMeta.close();
			return true;
		} catch (SQLException e) {
			System.out.println("****     " + e.getLocalizedMessage());
			return false;
		}

	}*/

}
