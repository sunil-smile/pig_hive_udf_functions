package com.simplyanalytics.hiveapp.udf;


import java.math.BigInteger;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.io.Text;

@Description(name="example_max", value="_FUNC_(expr) - Returns the maximum value of expr")
public class MaxTimeStamp  extends UDAF
{
    public static class MaxTimeStampEvaluator implements UDAFEvaluator {

            private BigInteger lMax = null;
            private Text tMax = null;
            private boolean mEmpty = false;
            public MaxTimeStampEvaluator() {
                init();
            }
            
            public void init() {
                this.lMax = null;
                this.tMax = null;
                this.mEmpty = true;
            }
            
            public boolean iterate(Text o) {
                if ((o != null)&&(o.toString().length()!=0)) {
                    BigInteger newDate = new BigInteger(o.toString().replaceAll("[^0-9]", ""));
                    if (this.mEmpty) {
                        this.lMax = newDate;
                        this.tMax = new Text(o);
                        this.mEmpty = false;
                    } else if (newDate.compareTo(this.lMax)>0) {
                        this.lMax = newDate;
                        this.tMax = new Text(o);
                    }
                }
                return true;
            }
            public Text terminatePartial() {
                return this.mEmpty ? null : this.tMax;
            }
            public boolean merge(Text o) {
                return iterate(o);
            }
            public Text terminate() {
                return this.mEmpty ? null : this.tMax;
            }
   
    
    
  }//end class  EVALAUTOR  
}//end class UDAF
