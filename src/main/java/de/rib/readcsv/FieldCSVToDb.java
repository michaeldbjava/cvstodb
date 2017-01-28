package de.rib.readcsv;

public class FieldCSVToDb {
	String cvsField;
	String dbField;
	String type;
	
	
	public FieldCSVToDb() {
		super();
		// TODO Auto-generated constructor stub
	}
	public FieldCSVToDb(String cvsField, String dbField) {
		super();
		this.cvsField = cvsField;
		this.dbField = dbField;
	}
	public String getCvsField() {
		return cvsField;
	}
	public void setCvsField(String cvsField) {
		this.cvsField = cvsField;
	}
	public String getDbField() {
		return dbField;
	}
	public void setDbField(String dbField) {
		this.dbField = dbField;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	

}
