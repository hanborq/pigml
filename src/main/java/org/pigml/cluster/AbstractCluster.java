package org.pigml.cluster;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.pig.LoadFunc;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.impl.util.UDFContext;
import org.pigml.utils.SimpleOutputFormat;
import org.pigml.utils.SimpleOutputFormat.Handler;
import org.pigml.utils.SimpleOutputFormat.SimpleRecordWriter;

import com.google.common.base.Preconditions;

public abstract class AbstractCluster implements StoreFuncInterface {
	static protected final Log LOG = LogFactory.getLog(AbstractCluster.class);
	private SimpleRecordWriter writer;
	
	abstract protected void preClose() throws IOException;
	
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

	@SuppressWarnings({ "rawtypes" })
	@Override
	public void prepareToWrite(RecordWriter writer) throws IOException {
		Preconditions.checkState(this.writer == null);
		this.writer = (SimpleRecordWriter) writer;
		this.writer.register(new Handler(){
			@Override
			public void onClose() throws IOException {
				preClose();
			}
		});
	}

	@Override
	public void setStoreFuncUDFContextSignature(String signature) {
	}

	@Override
	public void cleanupOnFailure(String location, Job job) throws IOException {
	}
	
	protected void persist(String type, Object... value) throws IOException {
		final String fileid = getRuntimeId() + "."+type;
		try {
			writer.write(fileid, String.valueOf(value[0]));
			for (int i=1; i<value.length; i++) {
				writer.write(fileid, ",");
				writer.write(fileid, String.valueOf(value[i]));
			}
			writer.write(fileid, "\n");
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
	}
	
	protected long tell(Object id) throws IOException {
		return writer.tell(id);
	}
	
	private String rtid;
	protected String getRuntimeId() {
		if (rtid == null) {
			rtid = TaskAttemptID.forName(UDFContext.getUDFContext().getJobConf().get("mapred.task.id"))
					.getTaskID().toString();
		}
		return rtid;
	}
	
	@Override
	public void cleanupOnSuccess(String location, Job job) throws IOException {
	}
}
