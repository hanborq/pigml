package org.pigml.cluster.km;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.pig.LoadFunc;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.pigml.utils.SimpleOutputFormat;


public class KMeanCentroidStore implements StoreFuncInterface {
	static final Log LOG = LogFactory.getLog(KMeanCentroidStore.class);
	private RecordWriter<Object, String> writer;
	
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
		ResourceFieldSchema[] fields = s.getFields();
		if (fields == null || fields.length < 2) {
			throw new IOException("expecting two field for schema");
		}
		if (fields[0].getType() != DataType.INTEGER) {
			throw new IOException("first field should be integer");
		}
		if (fields[1].getType() != DataType.BAG) {
			throw new IOException("second field should be bag");
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public void prepareToWrite(RecordWriter writer) throws IOException {
		this.writer = writer;
	}

	@Override
	public void putNext(Tuple t) throws IOException {
		if (t == null || t.size() < 2)
			return;
		Integer k = (Integer) t.get(0);
		DataBag bag = (DataBag) t.get(1);
		Iterator<Tuple> itor = bag.iterator();
		if (!itor.hasNext()) {
			return;
		}
		Tuple it = itor.next();
		if (it.size() < 2) {
			throw new IOException("expecting at least 2 items in tuple "+it);  //first item being the 'bucket' id
		}
		final Double[] sum = new Double[it.size() - 1];
		for (int i=0; i<sum.length; i++) {
			sum[i] = 0.0;
		}
		int count = 0;
		while (true) {
			count++;
			for (int i=0; i<sum.length; i++) {
				Double d = (Double)it.get(i + 1);
				sum[i] += d != null ? d : 0;
			}
			if (!itor.hasNext())
				break;
			it = itor.next();
			if (it.size() != sum.length + 1) {
				throw new IOException("expecting "+(sum.length+1)+" items in tuple "+it);
			}
		}
		for (int i=0; i<sum.length; i++) {
			sum[i] = sum[i] / count;
		}
		LOG.info("centoids "+ k + " " + StringUtils.join(sum, " "));
		try {
			writer.write(k + ".centroids", k + " " + StringUtils.join(sum, " ") + "\n");
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
	}

	@Override
	public void setStoreFuncUDFContextSignature(String signature) {
	}

	@Override
	public void cleanupOnFailure(String location, Job job) throws IOException {
	}

	@Override
	public void cleanupOnSuccess(String location, Job job) throws IOException {
		// TODO Auto-generated method stub
		
	}
	
	
}
