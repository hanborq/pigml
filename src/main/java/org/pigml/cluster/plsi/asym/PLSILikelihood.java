package org.pigml.cluster.plsi.asym;

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.Vector;
import org.apache.pig.ResourceSchema;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.pigml.storage.AbstractStore;
import org.pigml.storage.types.NullOutputFormat;
import org.pigml.utils.*;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: g
 * Date: 13-9-26
 * Time: 下午3:28
 * To change this template use File | Settings | File Templates.
 */
public class PLSILikelihood extends AbstractStore {

    private static final Log LOG = LogFactory.getLog(PLSILikelihood.class);
    private static final String DEFAULT_PART = "part-00000";
    private VectorUtils.Tuple2Vector vgen;
    private int n_z;

    public PLSILikelihood() throws IOException {
        super(new NullOutputFormat());
        Env.inBackground(new Env.BackgroundProcedure() {
            @Override
            public void execute(Configuration conf) throws IOException {
                Path savedAt = getStorePath();
                LOG.info("Loading model from previous iteration");
                Pz_d = IOUtils.loadMatrix(conf, PathUtils.enter(savedAt, "Pz_d", DEFAULT_PART));
                Pw_z = IOUtils.loadMatrix(conf, PathUtils.enter(savedAt, "Pw_z", DEFAULT_PART));
                vgen = VectorUtils.createVectorGenerator(
                        Env.getProperty(PLSILikelihood.class, "vector.type"));
                Preconditions.checkState(Pz_d.numCols() == Pw_z.numCols());
                n_z = Pz_d.numCols();
            }
        });
    }

    private Matrix Pz_d, Pw_z;

    private double likelihood;

    @Override
    public void putNext(Tuple tuple) throws IOException {
        int d = (Integer)tuple.get(0);
        Vector elmnts = vgen.apply((Tuple) tuple.get(1));
        /**
         * Per formula-3, hofmann 99
         */

        for (Vector.Element el : elmnts.nonZeroes()) {
            int w = el.index();
            double pdw = 0;
            for (int z=0; z<n_z; z++) {
                pdw += Pw_z.get(w, z) * Pz_d.get(d, z);
            }
            likelihood += el.get() * Math.log(pdw);
        }
    }

    //preprocess for M step (runs in background, together with E step)
    @Override
    protected void handleWriterClose(TaskAttemptContext ctx) throws IOException {
        Configuration conf = ctx.getConfiguration();
        BGFGUtils.savePartialState(
                PathUtils.enter(getStorePath(), "_LIKELIHOOD_"),
                conf,
                String.valueOf(likelihood));
    }

    @Override
    public void cleanupOnSuccess(String location, Job job) throws IOException {
        Path dir = PathUtils.enter(new Path(location), "_LIKELIHOOD_");
        Configuration conf = job.getConfiguration();
        double likeli = 0;
        for (String s : BGFGUtils.collectPartialStates(dir, conf)) {
            likeli += Double.valueOf(s);
        }
        IOUtils.writeText(conf, new Path(dir, "LAST"), String.valueOf(likeli));
        LOG.info("NEW LIKELIHOOD "+likeli);
    }

    @Override
    public void checkSchema(ResourceSchema s) throws IOException {
        SchemaUtils.claim(s, 0, DataType.INTEGER);
        ResourceSchema.ResourceFieldSchema tuple = SchemaUtils.claim(s, 1, DataType.TUPLE);
        Env.setProperty(PLSILikelihood.class, "vector.type", VectorUtils.typeOfVector(tuple.getSchema()));
    }

    @Override
    public void setStoreLocation(String location, Job job) throws IOException {
        super.setStoreLocation(location, job);
        FileSystem fs = FileSystem.get(job.getConfiguration());
        Preconditions.checkState(
                fs.exists(PathUtils.enter(new Path(location), "Pw_z", DEFAULT_PART)),
                "No model trained at %s", location);
    }
}
