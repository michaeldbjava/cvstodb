package de.rib.datehelper;

public class ConvertDateToIso {

	public ConvertDateToIso() {
		// TODO Auto-generated constructor stub
	}

	public static String convert(String date){
		String day = date.substring(0,2);
		String month=date.substring(3,5);
		String year=date.substring(6,10);
		String isodate=year+"-"+month+"-" + day;
		return isodate;
	}
}
