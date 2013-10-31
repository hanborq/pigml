package org.pigml.storage.vector;

import com.google.common.base.Preconditions;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
public class RollingVectorLoader extends SparseVectorLoader {
    static final Log LOG = LogFactory.getLog(RollingVectorLoader.class);

    public RollingVectorLoader() throws ParseException, IOException, ClassNotFoundException {
        this(null);
    }

    public RollingVectorLoader(String keyType) throws ParseException, IOException, ClassNotFoundException {
        super(keyType);
    }

    @Override
    public void setLocation(String location, Job job) throws IOException {
        int lastIteration = IterationUtils.getLastIteration(job.getConfiguration(), new Path(location));
        Preconditions.checkArgument(lastIteration >= 0, "No model iterated at %s", location);
        String latestModelLocation = location + Path.SEPARATOR + lastIteration;
        LOG.info("Load location for lastest rolling is " + latestModelLocation);
        super.setLocation(latestModelLocation, job);
    }
}
