package org.pigml.udf.text;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.pig.ResourceSchema;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.pigml.storage.ProxyStore;
import org.pigml.storage.writable.WritableStore;
import org.pigml.utils.Env;
import org.pigml.utils.IOUtils;
import org.pigml.utils.PathUtils;
import org.pigml.utils.SchemaUtils;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: g
 * Date: 13-9-26
 * Time: 下午3:28
 * To change this template use File | Settings | File Templates.
 */
public class DFStore extends ProxyStore implements Constants {

    private static final Log LOG = LogFactory.getLog(DFStore.class);

    public DFStore() throws ParseException, IOException, ClassNotFoundException {
        super(new WritableStore("int", "long"));
    }

    private Distribution distribution = new Distribution();

    @Override
    public void putNext(Tuple tuple) throws IOException {
        if (tuple != null) {
            super.putNext(tuple);
            int feature = (Integer)tuple.get(0);
            long frequency = (Long)tuple.get(1);
            distribution.update(feature, frequency);
        }
    }

    @Override
    public void checkSchema(ResourceSchema s) throws IOException {
        SchemaUtils.claim(s, 0, DataType.INTEGER);
        SchemaUtils.claim(s, 1, DataType.LONG);
    }

    @Override
    public void setStoreLocation(String location, Job job) throws IOException {
        super.setStoreLocation(location, job);
        Env.setProperty(DFStore.class, "storelocation", location);
    }

    @Override
    public void cleanupOnSuccess(String location, Job job) throws IOException {
        store.cleanupOnSuccess(location, job);
        Configuration conf = job.getConfiguration();
        FileSystem fs = FileSystem.get(conf);
        Iterable<Path> subDists = PathUtils.listPathByPrefix(fs, new Path(location), DISTRIBUTION_FILE);
        Distribution dist = new Distribution();
        for (Path path : subDists) {
            Path file = new Path(location, path.getName());
            dist = dist.overlay(Distribution.fromString(IOUtils.readLine(conf, file)));
            fs.delete(file, false);
        }
        Path file = new Path(location, DISTRIBUTION_FILE);
        IOUtils.writeText(conf, file, dist.toString());
        LOG.info("FINISHED "+file);
    }

    @Override
    protected void handleWriterClose(TaskAttemptContext context) {
        Path file = new Path(Env.getProperty(DFStore.class, "storelocation"),
                DISTRIBUTION_FILE +Env.getPartID());
        try {
            IOUtils.writeText(context.getConfiguration(), file,
                    distribution.toString());
            LOG.info("Created " + file + " with "+distribution);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static private final String DISTRIBUTION_FILE = "_FEATURE_DISTRIBUTION_";

    static public class Distribution {

        public long getMaxFrequency() {
            return maxFrequency;
        }

        private int maxFeature;
        private int numFeature;
        private long maxFrequency;
        private long totalFrequency;

        public Distribution() {
            this(0, 0, 0, 0);
        }

        public Distribution(int maxFeature, int numFeature, long maxFrequency, long totalFrequency) {
            this.maxFeature = maxFeature;
            this.numFeature = numFeature;
            this.maxFrequency = maxFrequency;
            this.totalFrequency = totalFrequency;
        }

        public Distribution overlay(Distribution that) {
            return new Distribution(Math.max(this.maxFeature, that.maxFeature),
                    this.numFeature + that.numFeature,
                    Math.max(this.maxFrequency, that.maxFrequency),
                    this.totalFrequency + that.totalFrequency);
        }

        public void update(int feature, long frequency) {
            maxFeature = Math.max(maxFeature, feature);
            maxFrequency = Math.max(maxFrequency, frequency);
            numFeature++;
            totalFrequency += frequency;
        }

        @Override
        public String toString() {
            return Joiner.on(",").join(maxFeature, numFeature, maxFrequency, totalFrequency);
        }

        static public Distribution fromString(String distribution) {
            String[] ss = Iterables.toArray(Splitter.on(",").split(distribution), String.class);
            return new Distribution(Integer.parseInt(ss[0]), Integer.parseInt(ss[1]),
                    Long.parseLong(ss[2]), Long.parseLong(ss[3]));
        }
    }

    static public Distribution getDistribution(Configuration conf, String location) throws IOException {
        Path file = new Path(location, DISTRIBUTION_FILE);
        return Distribution.fromString(IOUtils.readLine(conf, file));
    }
}
