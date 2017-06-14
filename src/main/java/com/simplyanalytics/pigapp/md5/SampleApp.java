package com.simplyanalytics.pigapp.md5;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.commons.codec.binary.Base64;

import com.simplyanalytics.pigapp.utils.PigAppUtil;

public class SampleApp {
	
	public static void main(String args[]) throws NoSuchAlgorithmException{
		  
		   DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
		   //get current date time with Date()
		   Date date = new Date();
		   //System.out.println(dateFormat.format(date)); don't print it, but save it!
		   String yourDate = dateFormat.format(date);
		   System.out.println(yourDate);

}
}
