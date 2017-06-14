package com.simplyanalytics.pigapp.customloader;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Sample {
	static String[] SOURCE_DATETIME_FORMAT = {"dd-MMM-yyyy HH:mm:ss","EEE MMM dd HH:mm:ss yyyy"};

	public static void main(String[] args) throws IOException {
		
		
		// TODO Auto-generated method stub
		/*System.out.println(convert2Date("04-JUN-2015 11:10:00","yyyy-MM-dd HH:mm:ss", SOURCE_DATETIME_FORMAT));
		System.out.println(convert2Date("Wed Apr 29 11:31:03 2015","yyyy-MM-dd HH:mm:ss", SOURCE_DATETIME_FORMAT));*/

		


		
	}

	public static String convert2Date(String dateValue,String convertToFormat, String[] sourceFormats) {
		String sourceFormat;

		if (dateValue == null || dateValue.isEmpty())
			return null;

		for (int i = 0; i < sourceFormats.length; i++) {
			sourceFormat = sourceFormats[i];
			//System.out.println(sourceFormat);
			try {
				SimpleDateFormat sourceDateFormat = new SimpleDateFormat(sourceFormat);
				SimpleDateFormat formattedDateFormat = new SimpleDateFormat(convertToFormat);
				Date inputdate = (Date) sourceDateFormat.parse(dateValue);
				//System.out.println(inputdate);
				String formattedate=formattedDateFormat.format(inputdate);
				return formattedate;
			} catch (ParseException e) {

			}
		}
		return null;
	}

}
