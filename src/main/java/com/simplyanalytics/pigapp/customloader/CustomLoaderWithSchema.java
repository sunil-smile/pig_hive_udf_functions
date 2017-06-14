package com.simplyanalytics.pigapp.customloader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;





import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.pig.Expression;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.util.Utils;

import com.simplyanalytics.pigapp.utils.PigAppUtil;



public class CustomLoaderWithSchema extends LoadFunc implements LoadMetadata {

	private String delim;
	private static final int DEFAULT_LIMIT = 226;
	private int limit = DEFAULT_LIMIT;
	private RecordReader reader;
	private List<Integer> indexes;
	private TupleFactory tupleFactory;
	private String schemafile =null;

	public CustomLoaderWithSchema(String... inputs) {
		this.indexes = new ArrayList<Integer>();
		int i=0;
		for (String values : inputs) {
			if(i==0){
				this.schemafile=values;
			}else if(i==1){
				this.delim = values;
			}else{			
			indexes.add(new Integer(values));
			}
			i++;
		}
		tupleFactory = TupleFactory.getInstance();
	}
	
	public CustomLoaderWithSchema(String schemafile,String delim) {
		this.schemafile=schemafile;
		this.delim = delim;
		tupleFactory = TupleFactory.getInstance();
	}

	@Override
	public InputFormat getInputFormat() throws IOException {
		return new TextInputFormat();
	}

	/**
	 * the input in this case is a TSV, so split it, make sure that the
	 * requested indexes are valid,
	 */
	@Override
	public Tuple getNext() throws IOException {
		Tuple tuple = null;
		List<String> values = new ArrayList<String>();
		try {
			boolean notDone = reader.nextKeyValue();
			if (!notDone) {
				return null;
			}
			Text value = (Text) reader.getCurrentValue();
			if (value != null) {
				//String parts[] = PigAppUtil.splitTotokens(value.toString(),delim);
				String parts[] = value.toString().split(delim,-1);
				for (Integer index : indexes) {
					if (index > limit) {
						throw new IOException("index " + index
								+ "is out of bounds: max index = " + limit);
					} else {
						values.add(parts[index]);
					}
				}
				tuple = tupleFactory.newTuple(values);
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return tuple;
	}

	@Override
	public void prepareToRead(RecordReader reader, PigSplit pigSplit)
			throws IOException {
		this.reader = reader; // note that for this Loader, we don't care about the PigSplit.
		}

	@Override
	public void setLocation(String location, Job job) throws IOException {
		FileInputFormat.setInputPaths(job, location); // the location is assumed to be comma separated paths.
	}

	@Override
	public String[] getPartitionKeys(String arg0, Job arg1) throws IOException {
		return null;
	}

	@Override
	public ResourceSchema getSchema(String location, Job job) throws IOException {
		FileSystem fs = FileSystem.get((new Configuration()));
		FSDataInputStream inputstream = fs.open(new Path(schemafile));
		String reqSchema;
		BufferedReader reader = new BufferedReader(new InputStreamReader(inputstream));
		String line = reader.readLine();
		
		String fields[] =  PigAppUtil.splitTotokens(line, ",");
		StringBuffer reqfields = new StringBuffer();
		for (Integer index : indexes) {

			if (index > limit) {
				throw new IOException("index " + index+ "is out of bounds: max index = " + limit);
			} else {
				reqfields.append(fields[index]+",");
			}
		}
		reqSchema = reqfields.substring(0, reqfields.length()-1);
		ResourceSchema schema = new ResourceSchema(Utils.getSchemaFromString(reqSchema));
		return schema;
	}

	@Override
	public ResourceStatistics getStatistics(String arg0, Job arg1)
			throws IOException {
		return null;
	}

	@Override
	public void setPartitionFilter(Expression arg0) throws IOException {
	
	}
	
	

}
