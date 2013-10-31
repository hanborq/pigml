package org.pigml.storage.vector;

import com.twitter.elephantbird.pig.load.SequenceFileLoader;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.pigml.common.Defines;
import org.pigml.lang.FX;
import org.pigml.storage.types.Converters;
import org.pigml.utils.IOUtils;
import org.pigml.utils.PathUtils;

import java.io.IOException;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: g
 * Date: 9/14/13
 * Time: 3:29 PM
 * To change this template use File | Settings | File Templates.
 */
public class VectorLoader extends SequenceFileLoader implements Defines {

    private static final Log LOG = LogFactory.getLog(VectorLoader.class);

    public VectorLoader() throws IOException, ParseException {
        this(null);
    }

    public VectorLoader(String keyType) throws IOException, ParseException {
        this(keyType, null, null);
    }

    public VectorLoader(String keyType, String valueArgs, String otherArgs) throws ParseException, IOException {
        super("-c " + Converters.get(FX.any(keyType, "int")),
                "-c " + Converters.get("vector") + formatValueArgs(valueArgs),
                FX.any(otherArgs, ""));
    }

    static String formatValueArgs(String valueArgs) {
        return (StringUtils.isEmpty(valueArgs) ? "" : " -- " + valueArgs);
    }

    @Override
    public void setLocation(String location, Job job) throws IOException {
        super.setLocation(location, job);
        Path[] dirs = FileInputFormat.getInputPaths(job);
        Configuration conf = job.getConfiguration();
        if (dirs.length > 0 && conf.getBoolean("pigml.input.autorecursive", true)) {
            List<Path> paths = PathUtils.resolveInputPaths(dirs[0], conf);
            for (int i=1; i<dirs.length; i++) {
                paths.addAll(PathUtils.resolveInputPaths(dirs[i], job.getConfiguration()));
            }
            FileInputFormat.setInputPaths(job, paths.toArray(new Path[0]));
            LOG.info("Paths to be processed: [" +
                    StringUtils.join(FileInputFormat.getInputPaths(job), ",") + "]");
        }
    }
}
