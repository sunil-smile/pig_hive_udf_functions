package com.simplyanalytics.pigapp.utils;

public class PigAppUtil {
	
	public static String[] splitTotokens(String line, String delim){
	    String str_newLine= line+delim;
	    
		String s = str_newLine;
	    int i = 0;

	    while (s.contains(delim)) {
	        s = s.substring(s.indexOf(delim) + delim.length());
	        i++;
	    }
	    
	    String token = null;
	    String remainder = null;
	    String[] tokens = new String[i];

	    for (int j = 0; j < i; j++) {
	        token = str_newLine.substring(0, str_newLine.indexOf(delim));
	        tokens[j] = token;
	        remainder = str_newLine.substring(str_newLine.indexOf(delim) + delim.length());
	        str_newLine = remainder;
	    }
	    return tokens;
	}

}
