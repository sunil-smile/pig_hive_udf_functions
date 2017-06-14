package com.simplyanalytics.pigapp.md5;

import java.io.IOException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;


public class Md5Hashing extends EvalFunc<String> {

	private MessageDigest md5er;
	String record;

	@Override
	public String exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0)
			return null;
		try {
			// convert tuple to string
			record = input.get(0).toString();

			// get the md5 for converted string
			md5er = MessageDigest.getInstance("md5");
			String md5val = (new BigInteger(1, md5er.digest(record.getBytes()))
					.toString(16));

			// input.append(md5val);
			return md5val;
		} catch (NoSuchAlgorithmException e) {
			throw new IOException("algorithm not exists ", e);
		} catch (Exception e) {
			throw new IOException("Caught exception processing input row ", e);
		} finally {

		}
	}

	@Override
	public Schema outputSchema(Schema input) {
		try {
			return new Schema(new Schema.FieldSchema("md5_hash_val", null, DataType.CHARARRAY));
		} catch (FrontendException e) {
			return null;
		}
	}

}