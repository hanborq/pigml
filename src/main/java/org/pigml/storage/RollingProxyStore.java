package org.pigml.storage;

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.pig.StoreFuncInterface;
import org.pigml.udf.text.Constants;
import org.pigml.utils.Env;
import org.pigml.utils.Functors;
import org.pigml.utils.IOUtils;
import org.pigml.utils.IterationUtils;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: g
 * Date: 13-9-26
 * Time: 下午3:28
 * To change this template use File | Settings | File Templates.
 */

public abstract class RollingProxyStore extends ProxyStore implements Constants {

    private static final Log LOG = LogFactory.getLog(RollingProxyStore.class);

    protected RollingProxyStore(StoreFuncInterface store) throws IOException {
        super(store);
    }

    @Override
    public void setStoreLocation(String location, Job job) throws IOException {
        Configuration conf = job.getConfiguration();
        int iteration = IterationUtils.allocIteration(conf, location);
        LOG.info(String.format("Iteration %d at %s", iteration, location));
        Env.setProperty(RollingProxyStore.class, "iterationhome", location);
        Env.setProperty(RollingProxyStore.class, "iterationid", String.valueOf(iteration));
        super.setStoreLocation(location + Path.SEPARATOR + iteration, job);
    }

    @Override
    public void cleanupOnFailure(String location, Job job) throws IOException {
        int iteration = IterationUtils.allocIteration(job.getConfiguration(), location);
        super.cleanupOnFailure(location + Path.SEPARATOR + iteration, job);
    }

    @Override
    public void cleanupOnSuccess(String location, Job job) throws IOException {
        Configuration conf = job.getConfiguration();
        int iteration = IterationUtils.allocIteration(job.getConfiguration(), location);
        IterationUtils.commitIteration(conf, location, iteration);
        super.cleanupOnSuccess(location + Path.SEPARATOR + iteration, job);
        LOG.info("FINISHED ITERATION " + iteration);
    }

    @Override
    protected void handleWriterClose(TaskAttemptContext ctx) {
    }
}
