package de.rib.datehelper;

import java.time.format.DateTimeFormatter;
import java.util.Date;

public class TestConvertDate {

	public TestConvertDate() {
		// TODO Auto-generated constructor stub
		
	}
	
	public static void main(String[] args){
		ConvertDateToIso cDI= new ConvertDateToIso();
		System.out.println(cDI.convert("20.05.2018"));
		
	}

}
