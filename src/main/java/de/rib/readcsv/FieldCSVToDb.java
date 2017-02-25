package de.rib.readcsv;

public class FieldCSVToDb {
	String cvsField;
	String dbField;
	int type;
	
	
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
	public int getType() {
		return type;
	}
	public void setType(int type) {
		this.type = type;
	}
	

}
