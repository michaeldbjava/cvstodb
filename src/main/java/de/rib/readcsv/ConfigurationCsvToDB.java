package de.rib.readcsv;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
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
	private Connection conToDb = null;
	private Document document = null;

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
			DocumentBuilder documentBuilder = dbFactory.newDocumentBuilder();
			document = documentBuilder.parse(xmlFile);

			/*
			 * Redundanter Code, Methode implementieren, die den redundaten Code
			 * neutralisiert
			 */
			/* Lese Element csvfile */
			NodeList nodePathCsvFile = document.getElementsByTagName("csvfile");
			Element elementCsvFile = (Element) nodePathCsvFile.item(0);
			String pathCSVFile = elementCsvFile.getFirstChild().getTextContent();
			this.setCvsfile(pathCSVFile);

			/*
			 * Dieser Versuch redundaten Kode zu eliminieren hat leider nicht
			 * funktioniert. this.setCvsfile(this.getValueOfXMLNode("cvsfile"));
			 */

			/* Lese Element convert-cvs-dates-to-iso-date */
			NodeList nodeDateToIso = document.getElementsByTagName("convert-cvs-dates-to-iso-date");
			Element elementIsoDate = (Element) nodeDateToIso.item(0);
			String flagDateToIso = elementIsoDate.getFirstChild().getTextContent();
			if (flagDateToIso != null) {
				if (flagDateToIso.equals("true"))
					this.setDateIsISODate(true);
				else {
					this.setDateIsISODate(false);
				}
			}

			/* Lese Element delimeter */
			NodeList nodeDelimeterCsvFile = document.getElementsByTagName("delimeter");
			Element elementDelimeter = (Element) nodeDelimeterCsvFile.item(0);
			String delimeterCSVFile = elementDelimeter.getFirstChild().getTextContent();
			this.setDelimeter(delimeterCSVFile.charAt(0));

			/* Lese Element dbtype */
			NodeList nodeDbType = document.getElementsByTagName("dbtype");
			Element elementDbType = (Element) nodeDbType.item(0);
			this.setDbtype(elementDbType.getFirstChild().getTextContent());

			/* Lese Element localhost */
			NodeList nodeLocalhost = document.getElementsByTagName("host");
			Element elementLocalhost = (Element) nodeLocalhost.item(0);
			this.setLocalhost(elementLocalhost.getFirstChild().getTextContent());

			/* Read DB Name */
			NodeList nodeDbName = document.getElementsByTagName("database_name");
			Element elementDbName = (Element) nodeDbName.item(0);
			this.setDatabase(elementDbName.getFirstChild().getTextContent());

			/* Read Username */
			NodeList nodeUserDb = document.getElementsByTagName("user");
			Element elementUserDb = (Element) nodeUserDb.item(0);
			this.setUser(elementUserDb.getFirstChild().getTextContent());

			/* Read Password */
			NodeList nodeUserPassword = document.getElementsByTagName("password");
			Element elementUserPassword = (Element) nodeUserPassword.item(0);
			this.setPassword(elementUserPassword.getFirstChild().getTextContent());

			/* Read DB Port */
			NodeList nodeDbPort = document.getElementsByTagName("port");
			Element elementDbPort = (Element) nodeDbPort.item(0);
			this.setPort(elementDbPort.getFirstChild().getTextContent());

			/* Read Table */

			NodeList nodeTable = document.getElementsByTagName("table");
			Element elementTable = (Element) nodeTable.item(0);
			this.setTable(elementTable.getFirstChild().getTextContent());

			/* Read cvs to db column mappings */
			// map-csv-fields-to-db
			NodeList nodeMapCsvToDb = document.getElementsByTagName("map-csv-fields-to-db");
			Element elementMapCsvToDb = (Element) nodeMapCsvToDb.item(0);
			NodeList nodeListMappingFields = elementMapCsvToDb.getElementsByTagName("field-mapping");
			//NodeList nodeListMappingExpression = elementMapCsvToDb.getElementsByTagName("calculated-value-mapping");
			int j = nodeListMappingFields.getLength();

			
			
			// conToDb=this.getConnectionToDb();
			for (int i = 0; i < j; i++) {

				Element elementFieldMapping = (Element) nodeListMappingFields.item(i);
				
				NodeList nodeCsvField = elementFieldMapping.getElementsByTagName("csv-field");
				NodeList nodeExpressionField = elementFieldMapping.getElementsByTagName("expression");
				NodeList nodeDbField = elementFieldMapping.getElementsByTagName("table-column");
				FieldCSVToDb fCvsToDb=null;
				if(nodeCsvField.getLength()!=0){
					System.out.println("Es handelt sich um ein CSV Feld");
				 fCvsToDb = new FieldCSVToDb(nodeCsvField.item(0).getFirstChild().getTextContent(),
						nodeDbField.item(0).getFirstChild().getTextContent());
				}
				else if(nodeExpressionField.getLength()!=0){
					System.out.println("Es handelt sich um eine Expression");
					fCvsToDb = new FieldCSVToDb(nodeExpressionField.item(0).getFirstChild().getTextContent(),
							nodeDbField.item(0).getFirstChild().getTextContent());
				}
				/*
				 * try { System.out.
				 * println("Verbindung zur Datenbank ist geschlossen: " +
				 * conToDb.isClosed()); } catch (SQLException e) {
				 * e.printStackTrace(); }
				 */
				if(nodeCsvField!=null && nodeCsvField.getLength()!=0)
				System.out.println(nodeCsvField.item(0).getFirstChild().getTextContent() + "-->"
						+ nodeDbField.item(0).getFirstChild().getTextContent());
				
				if(nodeExpressionField!=null && nodeExpressionField.getLength()!=0){
					System.out.println("Content Node Expression: " + nodeExpressionField.item(0).getFirstChild().getTextContent());
					System.out.println("Content Node dbfield: " + nodeDbField.item(0).getFirstChild().getTextContent());
					System.out.println(nodeExpressionField.item(0).getFirstChild().getTextContent() + "-->"
							+ nodeDbField.item(0).getFirstChild().getTextContent());
										
				}
					
				this.addMapField(fCvsToDb);
			}

			System.out.println("Ausgelesener Pfad aus XML Datei: " + this.getCvsfile());
			System.out.println("Datenbank aus XML Datei: " + this.getDatabase());
			System.out.println("Datenbank Typ aus XML Datei: " + this.getDbtype());
			System.out.println("Datenbanknutzer aus XML Datei lesen: " + this.getUser());
			System.out.println("Passwort aus XML Datei lesen: " + this.getPassword());
			System.out.println("Datenbank Port aus XML Datei lesen: " + this.getPort());
			System.out.println("Datenbank Tabelle aus XML Datei lesen: " + this.getTable());
		} catch (IOException ioe) {
			ioe.printStackTrace();
		} catch (SAXException se) {
			se.printStackTrace();
		} catch (ParserConfigurationException pe) {
			pe.printStackTrace();
		}

		return true;

	}

	public Connection getConnectionToDb() {
		/* Auch hier redundaten Kode neutralisieren */
		if (this.getDbtype().equals("mysql")) {

			try {
				conToDb = DriverManager.getConnection("jdbc:mysql://" + this.localhost + ":" + this.getPort() + "/"
						+ this.getDatabase() + "?" + "user=" + this.getUser() + "&password=" + this.getPassword());

			} catch (SQLException ex) {
				// handle any errors
				System.out.println("SQLException: " + ex.getMessage());
				System.out.println("SQLState: " + ex.getSQLState());
				System.out.println("VendorError: " + ex.getErrorCode());
			}
			return conToDb;
		}

		if (this.getDbtype().equals("sqlite")) {

			try {
				conToDb = DriverManager.getConnection("jdbc:sqlite://" + this.getDatabase());

			} catch (SQLException ex) {
				// handle any errors
				System.out.println("SQLException: " + ex.getMessage());
				System.out.println("SQLState: " + ex.getSQLState());
				System.out.println("VendorError: " + ex.getErrorCode());
			}
			return conToDb;
		}
		return null;

	}

	private String getValueOfXMLNode(String xmlNode) {
		String xmlNodeValue = xmlNode;
		/* Die Methode funktioniert noch nicht */

		NodeList nodeOfTag = document.getElementsByTagName(xmlNodeValue);
		Element elementOfTag = (Element) nodeOfTag.item(0);
		String valueOfElement = elementOfTag.getFirstChild().getTextContent();
		return valueOfElement;
	}

}
