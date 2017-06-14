package com.simplyanalytics.hiveapp.udf;

import java.util.Date;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.Description;
@Description(name = "Getting Greater Date",value = "_FUNC_(date1,date2) - if date1 is equal or greater value,it returns true else false.",extended = "Example:\n"+ " > SELECT _FUNC_(date1_string,date2_string) FROM src;\n"+ " > select * from policy_lu_last_extract where lgcy_pol_number ='0192972' and GreaterUHGDate('2014-05-06-12.34.45.562234','2014-05-06-12.34.45.562228');")
public class CompareTimeStamp extends UDF{
	
	private SimpleDateFormat df;
	
	/*public CompareTimeStamp(){
	df = new SimpleDateFormat("yyyy-MM-dd'-'HH.mm.ss");
	}*/
	
	
	public String evaluate(String date1,String date2)
	{
		String maxTs=null;
		try
		{
		
		//Long date1inmills = df.parse(date1.toString().substring(0,date1.toString().lastIndexOf("."))).getTime();
		//Long date2inmills = df.parse(date2.toString().substring(0,date2.toString().lastIndexOf("."))).getTime();
		
		//BigDecimal date1value = new BigDecimal(date1inmills+"."+date1.toString().substring(date1.toString().lastIndexOf(".")+1,date1.length()));
		//BigDecimal date2value = new BigDecimal(date2inmills+"."+date2.toString().substring(date2.toString().lastIndexOf(".")+1,date2.length()));
		BigInteger date1value = new BigInteger(date1.replaceAll("[^0-9]", ""));
		BigInteger date2value = new BigInteger(date2.replaceAll("[^0-9]", ""));
		
		if(date1value.compareTo(date2value)>0){
			maxTs=date1;
		}else if(date1value.compareTo(date2value)==0){
			maxTs=date1;
		}else{
			maxTs=date2;
		}
			
		}catch(Exception ex)
		{
			maxTs="incorrect TS";
		}
		return maxTs;
	}
	

}//end class
