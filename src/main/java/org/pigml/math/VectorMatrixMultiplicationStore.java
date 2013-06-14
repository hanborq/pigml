package org.pigml.math;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.pig.ResourceSchema;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.data.Tuple;

public class VectorMatrixMultiplicationStore implements StoreFuncInterface {

	@Override
	public String relToAbsPathForStoreLocation(String location, Path curDir)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public OutputFormat getOutputFormat() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setStoreLocation(String location, Job job) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void checkSchema(ResourceSchema s) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepareToWrite(RecordWriter writer) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void putNext(Tuple t) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setStoreFuncUDFContextSignature(String signature) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void cleanupOnFailure(String location, Job job) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void cleanupOnSuccess(String location, Job job) throws IOException {
		// TODO Auto-generated method stub
		
	}
}
