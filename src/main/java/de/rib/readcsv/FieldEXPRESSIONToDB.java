package de.rib.readcsv;

public class FieldEXPRESSIONToDB {
	private String expressionValue;
	
	private String tableColumn;
	

	public FieldEXPRESSIONToDB() {
		// TODO Auto-generated constructor stub
	}

	
	public FieldEXPRESSIONToDB(String expressionValue, String tableColumn) {
		super();
		this.expressionValue = expressionValue;
		this.tableColumn = tableColumn;
	}


	public String getExpression() {
		return expressionValue;
	}

	public void setExpression(String expression) {
		this.expressionValue = expression;
	}

	public String getTableColumn() {
		return tableColumn;
	}

	public void setTableColumn(String tableColumn) {
		this.tableColumn = tableColumn;
	}
	
	

}
