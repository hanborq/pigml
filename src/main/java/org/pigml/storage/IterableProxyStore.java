package org.pigml.storage;

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.StoreFuncInterface;
import org.pigml.udf.text.Constants;
import org.pigml.utils.*;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: g
 * Date: 13-9-26
 * Time: 下午3:28
 * To change this template use File | Settings | File Templates.
 */

/**
 * This is used to facilitate iterating over some pre-computed MapReduce results.
 * Each 'part-xxxxx' in the MR results is considered an iteration.
 */
public abstract class IterableProxyStore extends ProxyStore implements Constants {

    private static final Log LOG = LogFactory.getLog(IterableProxyStore.class);
    private static final String ITERATE_OVER = "iterover";

    protected IterableProxyStore(StoreFuncInterface store) throws IOException {
        super(store);
        Env.inBackground(new Env.BackgroundProcedure() {
            @Override
            public void execute(Configuration conf) throws IOException {
                String iterover = Env.getJobDefine(conf, ITERATE_OVER);
                int iter = Env.getPropertyAs(IterableProxyStore.class, "iterationid", Functors.AS_INT);
                forIteration(conf, PathUtils.getParts(conf, iterover, iter), iterover);
            }
        });
    }

    protected abstract void forIteration(Configuration conf, Path partfile, String iterover) throws IOException;

    @Override
    public void setStoreLocation(String location, Job job) throws IOException {
        Configuration conf = job.getConfiguration();
        String iterover = Env.getJobDefine(conf, ITERATE_OVER);
        int iteration = IterationUtils.allocIteration(conf, location);
        int totalIteration = PathUtils.countParts(conf, iterover);
        LOG.info(String.format("Iteration %d at %s (Total dict part %d)",
                iteration, location, totalIteration));
        Preconditions.checkState(totalIteration>0, "Iteration over %s?", iterover);
        if (iteration >= totalIteration) {
            throw new IOException("ITERATION FINISHED ALREADY!");
        }
        Env.setProperty(IterableProxyStore.class, "iterationhome", location);
        Env.setProperty(IterableProxyStore.class, "iterationid", String.valueOf(iteration));
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
        int totalIteration = PathUtils.countParts(conf, Env.getJobDefine(conf, ITERATE_OVER));
        if (iteration >= totalIteration-1) {
            Path marker = new Path(location, ITERATION_DONE_FILE);
            marker.getFileSystem(conf).create(marker);
            LOG.info("MARK "+marker);
        } else {
            LOG.info("MORE ITERATION REQUIRED");
        }
        super.cleanupOnSuccess(location + Path.SEPARATOR + iteration, job);

    }
}
