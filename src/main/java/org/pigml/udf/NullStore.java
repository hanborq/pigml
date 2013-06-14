package org.pigml.udf;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.pig.LoadFunc;
import org.apache.pig.ResourceSchema;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.data.Tuple;
import org.pigml.utils.SimpleOutputFormat;


public class NullStore implements StoreFuncInterface {
	static final Log LOG = LogFactory.getLog(NullStore.class);
	@Override
	public String relToAbsPathForStoreLocation(String location, Path curDir)
			throws IOException {
		return LoadFunc.getAbsolutePath(location, curDir);
	}

	@Override
	public SimpleOutputFormat getOutputFormat() throws IOException {
		return new SimpleOutputFormat();
	}

	@Override
	public void setStoreLocation(String location, Job job) throws IOException {
		SimpleOutputFormat.setOutputPath(job, location);
	}

	@Override
	public void checkSchema(ResourceSchema s) throws IOException {
	}

	@Override
	public void putNext(Tuple t) throws IOException {
	}

	@Override
	public void setStoreFuncUDFContextSignature(String signature) {
	}

	@Override
	public void cleanupOnFailure(String location, Job job) throws IOException {
	}

	@Override
	public void prepareToWrite(@SuppressWarnings("rawtypes") RecordWriter writer) throws IOException {
	}
	
	@Override
	public void cleanupOnSuccess(String location, Job job) throws IOException {
	}
}
