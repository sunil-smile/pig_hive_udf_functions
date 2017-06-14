package com.simplyanalytics.pigapp.md5;

import java.io.IOException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

public class Md5Hash extends EvalFunc<Tuple> {

	private MessageDigest md5er;
	String record;

	@Override
	public Tuple exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0)
			return null;
		try {
			// convert tuple to string
			record = input.get(0).toString();

			// get the md5 for converted string
			md5er = MessageDigest.getInstance("md5");
			String md5val = (new BigInteger(1, md5er.digest(record.getBytes()))
					.toString(16));

			Tuple output = TupleFactory.getInstance().newTuple(2);
			output.set(0, md5val);
			output.set(1, input.get(0));

			return output;
		} catch (NoSuchAlgorithmException e) {
			throw new IOException("algorithm not exists ", e);
		} catch (Exception e) {
			throw new IOException("Caught exception processing input row ", e);
		}
	}

	public Schema outputSchema(Schema input) {
		try {
			FieldSchema md5_fld_schema = new FieldSchema("md5_hash_val",
					DataType.CHARARRAY);
			FieldSchema input_fld_schema = input.getFields().get(0);

			Schema finalSchema = new Schema();
			finalSchema.add(md5_fld_schema);
			finalSchema.add(input_fld_schema);

			return new Schema(new Schema.FieldSchema("md5_tuple_schema",
					finalSchema, DataType.TUPLE));
		} catch (FrontendException e) {
			/* //throw new FrontendException("error in schema"); */
			e.printStackTrace();
			return null;
		}

	}

}