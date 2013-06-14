package org.pigml.utils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

public class SimpleOutputFormat extends OutputFormat<Object, String> {
	
	public interface Handler {
		void onClose() throws IOException;
	}

	@Override
	public RecordWriter<Object, String> getRecordWriter(
			TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new SimpleRecordWriter(context.getConfiguration());
	}

	@Override
	public void checkOutputSpecs(JobContext context) throws IOException,
			InterruptedException {
	}

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		return new FileOutputCommitter(null, context);
	}
	
	static public void setOutputPath(Job job, String path) {
		job.getConfiguration().set("mapred.output.dir", path);
	}
	
	static public class SimpleRecordWriter extends RecordWriter<Object, String> {
		private final Configuration configuration;
		private final Map<Object, FSDataOutputStream> outs;
		private List<Handler> handlers;
		public SimpleRecordWriter(Configuration configuration) {
			this.configuration = configuration;
			this.outs = new HashMap<Object, FSDataOutputStream>();
			this.handlers = new ArrayList<Handler>(1);
		}

		@Override
		public void write(Object id, String value) throws IOException,
				InterruptedException {
			FSDataOutputStream out = outs.get(id);
			if (out == null) {
				FileSystem fs = FileSystem.get(configuration);
				Path file = new Path(configuration.get("mapred.output.dir") + Path.SEPARATOR + id);
				if (fs.exists(file)) {
					fs.delete(file, true);
				}
				out = fs.create(file, false);
				outs.put(id, out);
			}
			out.write(value.getBytes());
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException,
				InterruptedException {
			for (Handler h : handlers) {
				h.onClose();
			}
			for (DataOutputStream out : outs.values()) {
				out.close();
			}
			outs.clear();
		}
		
		public long tell(Object id) throws IOException {
			FSDataOutputStream stream = outs.get(id);
			return stream != null ? stream.getPos() : 0;
		}
		
		public void register(Handler handler) {
			handlers.add(handler);
		}
		
	}
	
}
