package org.pigml.classify;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.pig.LoadFunc;
import org.apache.pig.ResourceSchema;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;


public abstract class AbstractTrainer implements StoreFuncInterface {
	static final Log LOG = LogFactory.getLog(AbstractTrainer.class);
	private RecordWriter<NullWritable, String> writer;
	private byte estimatedType = DataType.UNKNOWN;
	
	protected abstract void trainModel(Object label, @SuppressWarnings("rawtypes") Map features);
	protected abstract Class<? extends BaseModel> getModelClass();
	protected abstract void saveModel();
	protected byte getPredictType() {
		return estimatedType;
	}
	
	@Override
	public String relToAbsPathForStoreLocation(String location, Path curDir)
			throws IOException {
		return LoadFunc.getAbsolutePath(location, curDir);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public OutputFormat getOutputFormat() throws IOException {
		return new TextOutputFormat<NullWritable, String>() {
			@SuppressWarnings("unchecked")
			@Override
			public RecordWriter 
	         getRecordWriter(TaskAttemptContext job
	                         ) throws IOException, InterruptedException {
				final RecordWriter innerWriter = super.getRecordWriter(job);
				return new RecordWriter() {

					@Override
					public void write(Object key, Object value)
							throws IOException, InterruptedException {
						innerWriter.write(key, value);
					}

					@Override
					public void close(TaskAttemptContext context)
							throws IOException, InterruptedException {
						LOG.info("close RecordWriter");
						persist(getModelClass().getName());
						persist(getPredictType());
						saveModel();
						innerWriter.close(context);
					}
					
				};
			}
		};
	}

	@Override
	public void setStoreLocation(String location, Job job) throws IOException {
		FileOutputFormat.setOutputPath(job, new Path(location));
	}

	@Override
	public void checkSchema(ResourceSchema s) throws IOException {
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void prepareToWrite(RecordWriter writer) throws IOException {
		this.writer = writer;
	}
	
	@SuppressWarnings({ "rawtypes" })
	@Override
	public void putNext(Tuple t) throws IOException {
		if (t == null || t.size() != 2) {
			LOG.error("malformed tuple "+t);
			return;
		}
		Object label = t.get(0);
		Map features = (Map) t.get(1);
		if (label == null || features == null || features.size() == 0) {
			return;
		}
		if (estimatedType == DataType.UNKNOWN) {
			estimatedType = DataType.findType(label);
		}
		trainModel(label, features);
	}
	
	protected void persist(Object... paras) {
		try {
			writer.write(NullWritable.get(), StringUtils.join(paras, ","));
		} catch (Exception e) {
			LOG.error("", e);
		}
	}
	
	protected void persist(Collection<Object> paras) {
		try {
			writer.write(NullWritable.get(), StringUtils.join(paras, ","));
		} catch (Exception e) {
			LOG.error("", e);
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
	}
}
