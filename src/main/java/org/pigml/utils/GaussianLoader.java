package org.pigml.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class GaussianLoader extends LoadFunc {
	static final Log LOG = LogFactory.getLog(GaussianLoader.class);
	private final TupleFactory fac = TupleFactory.getInstance();
	private RecordReader<NullWritable, List<Double>> reader;
	
	@Override
	public void setLocation(String location, Job job) throws IOException {
		job.getConfiguration().set("foo.location", location);
	}
	
	@Override
	public String relativeToAbsolutePath(String location, Path curDir) 
            throws IOException {      
        return location;
    }

	@Override
	public InputFormat<NullWritable, List<Double>> getInputFormat() throws IOException {
		return new FooInputFormat();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void prepareToRead(RecordReader reader, PigSplit split)
			throws IOException {
		this.reader = reader;
	}

	@Override
	public Tuple getNext() throws IOException {
		try {
			if (!reader.nextKeyValue())
				return null;
			return fac.newTupleNoCopy(reader.getCurrentValue());
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
	}
	
	static class FooInputSplit extends InputSplit implements Writable {
		String info;
		FooInputSplit() { //for reflection
		}
		public FooInputSplit(String s) {
			this.info = s;
		}

		@Override
		public long getLength() throws IOException, InterruptedException {
			return Long.MAX_VALUE;
		}

		@Override
		public String[] getLocations() throws IOException, InterruptedException {
			return new String[]{};
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeUTF(info);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			info = in.readUTF();
		}
		
	}
	
	static class FooInputFormat extends InputFormat<NullWritable, List<Double>> {

		@Override
		public List<InputSplit> getSplits(JobContext context) throws IOException,
				InterruptedException {
			List<InputSplit> result = new ArrayList<InputSplit>();
			String[] group = context.getConfiguration().get("foo.location").split("\\s+");
			for (String s : group) {
				result.add(new FooInputSplit(s));
			}
			return result;
		}

		@Override
		public RecordReader<NullWritable, List<Double>> createRecordReader(InputSplit split,
				TaskAttemptContext context) throws IOException,
				InterruptedException {
			return new FooGenerator();
		}
		
	}
	
	static class FooGenerator extends RecordReader<NullWritable, List<Double>> {
		private final Random random = new Random();
		private long count;
		private long generated;
		private double[] means;
		private double[] vars;
		private List<Double> value;
		
		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			String[] infos = StringUtils.split(((FooInputSplit)split).info, ";");
			this.count = Long.valueOf(infos[0]);
			this.means = new double[infos.length - 1];
			this.vars = new double[infos.length - 1];
			for (int i=0; i<means.length; i++) {
				String[] ss = StringUtils.split(infos[i + 1], ",");
				means[i] = Double.valueOf(ss[0]);
				vars[i] = Double.valueOf(ss[1]);
			}
			generated = 0;
		}

		
		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if (generated < count) {
				value = new ArrayList<Double>(means.length);
				for (int i=0; i<means.length; i++) {
					value.add(random.nextGaussian() * vars[i] + means[i]);
				}
				generated++;
				return true;
			}
			return false;
		}

		@Override
		public NullWritable getCurrentKey() throws IOException,
				InterruptedException {
			return NullWritable.get();
		}

		@Override
		public List<Double> getCurrentValue() throws IOException,
				InterruptedException {
			return value;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return generated / count;
		}

		@Override
		public void close() throws IOException {
		}
		
	}

}
