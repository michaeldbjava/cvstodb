package de.rib.datehelper;

public class TestConvertDate {

	public TestConvertDate() {
		// TODO Auto-generated constructor stub
		
	}
	
	public static void main(String[] args){
		ConvertDateToIso cDI= new ConvertDateToIso();
		System.out.println(cDI.convert("20.05.2018"));
	}

}
