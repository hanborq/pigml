package org.pigml.storage.vector;

import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.pigml.utils.IOUtils;
import org.pigml.utils.IterationUtils;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: g
 * Date: 9/14/13
 * Time: 5:14 PM
 * To change this template use File | Settings | File Templates.
 */
public class RollingVectorStore extends VectorStore {
    static final Log LOG = LogFactory.getLog(RollingVectorStore.class);

    public RollingVectorStore() throws ParseException, IOException, ClassNotFoundException {
    }

    @Override
    public void setStoreLocation(String location, Job job) throws IOException {
        Configuration conf = job.getConfiguration();
        int iteration = IterationUtils.allocIteration(conf, location);
        String newModelLocation = location + Path.SEPARATOR + iteration;
        FileSystem fs = FileSystem.get(conf);
        if (conf.getBoolean(PIGML_AUTOCLEANROLLING, false) && fs.exists(new Path(newModelLocation))) {
            LOG.info("Clean existing model rolling " + newModelLocation + " before get start");
            IOUtils.delete(new Path(newModelLocation), conf);
        }
        LOG.info("Store location for this rolling is "+newModelLocation);
        job.getConfiguration().set("pigml.model.storelocation", newModelLocation);
        super.setStoreLocation(newModelLocation, job);
    }

    @Override
    public void cleanupOnFailure(String location, Job job) throws IOException {
        int iteration = IterationUtils.allocIteration(job.getConfiguration(), location);
        super.cleanupOnFailure(location + Path.SEPARATOR + iteration, job);
        LOG.info("FAILED ITERATION " + iteration);
    }

    @Override
    public void cleanupOnSuccess(String location, Job job) throws IOException {
        Configuration conf = job.getConfiguration();
        int iteration = IterationUtils.allocIteration(job.getConfiguration(), location);
        IterationUtils.commitIteration(conf, location, iteration);
        super.cleanupOnSuccess(location + Path.SEPARATOR + iteration, job);
        LOG.info("SUCCESS ITERATION "+iteration);
    }
}
