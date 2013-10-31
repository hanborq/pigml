package org.pigml.cluster.plsi.asym;

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.mahout.math.*;
import org.apache.mahout.math.function.Functions;
import org.apache.pig.ResourceSchema;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.UDFContext;
import org.pigml.storage.AbstractStore;
import org.pigml.storage.types.NullOutputFormat;
import org.pigml.utils.*;

import java.io.IOException;

/**
 * Thomas Hofmann: Unsupervised Learning by Probabilistic Latent Semantic Analysis
 *   Machine Learning, 42, 177–196, 2001
 */
public class PLSITrain extends AbstractStore {

    private static final Log LOG = LogFactory.getLog(PLSITrain.class);
    private static final String DEFAULT_PART = "part-00000";
    private VectorUtils.Tuple2Vector vgen;
    private int PARALLEL = 0;

    public PLSITrain(String nz, String nd, String nw, String parallel) throws IOException {
        super(new NullOutputFormat());
        this.n_d = Integer.valueOf(nd); //TODO figure these out from job conf
        this.n_w = Integer.valueOf(nw);
        this.n_z = Integer.valueOf(nz);
        this.PARALLEL = Integer.valueOf(parallel);
        Env.inBackground(new Env.BackgroundProcedure() {
            @Override
            public void execute(Configuration conf) throws IOException {
                vgen = VectorUtils.createVectorGenerator(
                        Env.getProperty(PLSITrain.class, "vector.type"));
            }
        });
    }

    private int n_w;
    private int n_z, n_d;
    private PartTrainer trainer;

    private boolean initBackground(Vector terms) throws IOException {
        if (terms.getNumNonZeroElements() == 0) {
            return false;
        }
        int actualPart = terms.nonZeroes().iterator().next().index() % PARALLEL;
        if (trainer != null && trainer.actualPart == actualPart) {
            return true;
        }
        Configuration conf = UDFContext.getUDFContext().getJobConf();
        closeCurrentPart(conf);
        FileSystem fs = FileSystem.get(conf);
        Path savedAt = getStorePath();
        Matrix lastPz_d, lastPw_z;
        if (fs.exists(PathUtils.enter(savedAt, "Pz_d", DEFAULT_PART))) {
            LOG.info("Loading model from previous iteration");
            lastPz_d = IOUtils.loadMatrix(conf, PathUtils.enter(savedAt, "Pz_d", DEFAULT_PART));
            lastPw_z = IOUtils.loadMatrix(conf, PathUtils.enter(savedAt, "Pw_z", DEFAULT_PART));
        } else {
            LOG.info("Generating model for first run");
            lastPz_d = MathUtils.matrix.rand(n_d, n_z);
            lastPw_z = MathUtils.matrix.rand(n_w, n_z);
        }
        trainer = new PartTrainer(actualPart, lastPz_d, lastPw_z);
        return true;
    }

    @Override
    public void putNext(Tuple tuple) throws IOException {
        if (tuple == null) {
            return;
        }
        int d = (Integer)tuple.get(0);
        Vector terms = vgen.apply((Tuple) tuple.get(1));
        if (initBackground(terms)) {
            trainer.process(d, terms);
        }
    }

    @Override
    protected void handleWriterClose(TaskAttemptContext ctx) throws IOException {
        closeCurrentPart(ctx.getConfiguration());
    }

    @Override
    public void setStoreLocation(String location, Job job) throws IOException {
        super.setStoreLocation(location, job);
        int iter = IterationUtils.allocIteration(job.getConfiguration(), location);
        Env.setProperty(PLSITrain.class, "allocated.iteration", String.valueOf(iter));
    }

    @Override
    public void cleanupOnSuccess(String location, Job job) throws IOException {
        final Configuration conf = job.getConfiguration();
        final FileSystem fs = FileSystem.get(conf);
        final int iter = IterationUtils.allocIteration(conf, location);
        Path storeLocation = new Path(location);
        Path loadFrom = PathUtils.enter(storeLocation, "."+iter);
        Path saveTo = PathUtils.enter(storeLocation);

        //TODO single point operation. rearchitect for parallel

        Matrix merge = null;

        LOG.info("Merging, normalizing, saving Pw_z");

        for (Path path : PathUtils.listPath(fs, PathUtils.enter(loadFrom, "Pw_z"))) {
            merge = MathUtils.matrix.sum(IOUtils.loadMatrix(conf, path), merge);
        }
        Preconditions.checkState(merge.rowSize() == n_w && merge.columnSize() == n_z);

        MathUtils.matrix.normalizeRows(merge);
        IOUtils.saveMatrix(PathUtils.enter(saveTo, "Pw_z", DEFAULT_PART), conf, merge);

        LOG.info("Merging, normalizing, saving Pz_d");
        merge = null;
        for (Path path : PathUtils.listPath(fs, PathUtils.enter(loadFrom, "nd"))) {
            merge = MathUtils.matrix.sum(IOUtils.loadMatrix(conf, path), merge);
        }
        Preconditions.checkState(merge.rowSize() == 1 && merge.columnSize() == n_d);
        Vector ndVector = merge.viewRow(0);
        merge = null;
        for (Path path : PathUtils.listPath(fs, PathUtils.enter(loadFrom, "Pz_d"))) {
            merge = MathUtils.matrix.sum(IOUtils.loadMatrix(conf, path), merge);
        }
        Preconditions.checkState(merge.rowSize() == n_d && merge.columnSize() == n_z);
        for (int d=0; d<n_d; d++) {
            for (int z=0; z<n_z; z++) {
                if (merge.getQuick(d, z) > 0) {
                    merge.setQuick(d, z, merge.getQuick(d, z) / ndVector.get(d));
                }
            }
        }
        IOUtils.saveMatrix(PathUtils.enter(saveTo, "Pz_d", DEFAULT_PART), conf, merge);

        IterationUtils.commitIteration(conf, location, iter);

        LOG.info("FINISHED current iteration at "+storeLocation);
    }

    @Override
    public void checkSchema(ResourceSchema s) throws IOException {
        SchemaUtils.claim(s, 0, DataType.INTEGER);
        ResourceSchema.ResourceFieldSchema tuple = SchemaUtils.claim(s, 1, DataType.TUPLE);
        Env.setProperty(PLSITrain.class, "vector.type", VectorUtils.typeOfVector(tuple.getSchema()));
    }

    private void closeCurrentPart(Configuration conf) throws IOException {
        if (trainer != null) {
            trainer.close(conf);
            trainer = null;
        }
    }

    private class PartTrainer {
        private Matrix Pz_d; //dense
        private Matrix Pw_z; //dense
        private final int actualPart;
        private Pzdw Pz_dw;
        private Vector ndVector;

        public PartTrainer(int actualPart, Matrix lastPz_d, Matrix lastPw_z) {
            /**
             * Calcuate Pz_dw per Formula-6, Hofmann 01.
             *
             * About slice:
             *   Slice in the space of words is to reduce memory usage which is necessary.
             *   The input documents should be sliced the same way as model 'Pw_z' here.
             *
             * TBD will be more efficient if slice at saving
             */
            this.actualPart = actualPart;
            this.Pz_dw = new PzdwNative(actualPart, lastPz_d, lastPw_z);
            Pw_z = new SparseRowMatrix(n_w, n_z);
            for (int w = actualPart; w < n_w; w+=PARALLEL) {
                Pw_z.assignRow(w, new DenseVector(n_z));
            }
            Pz_d = new DenseMatrix(n_d, n_z);
            ndVector = new DenseVector(n_d);
            LOG.info("Finished pre-mstep for part "+actualPart);
        }

        public void process (int d, Vector terms) {
            validateSlice(terms);
            for (int z=0; z<n_z; z++) {
                Vector pzdw = Pz_dw.apply(z, d, terms);
                Pw_z.viewColumn(z).assign(pzdw, Functions.PLUS); //Formula-11 un-normalized
                Pz_d.set(d, z, pzdw.zSum());                     //Formula-12 un-normalized
            }
            ndVector.set(d, terms.getNumNonZeroElements());
        }

        public void close(Configuration conf) throws IOException {
            LOG.info("Closing writer for part "+actualPart);
            int iter = Integer.parseInt(Env.getProperty(PLSITrain.class, "allocated.iteration"));
            Path saveTo = PathUtils.enter(getStorePath(), "." + iter);
            IOUtils.saveMatrix(PathUtils.enter(
                    saveTo, "Pz_d", String.valueOf(actualPart)), conf, Pz_d);
            IOUtils.saveMatrix(PathUtils.enter(
                    saveTo, "Pw_z", String.valueOf(actualPart)), conf, Pw_z);
            IOUtils.saveMatrix(PathUtils.enter(
                    saveTo, "nd", String.valueOf(actualPart)), conf, MathUtils.asMatrix(ndVector));
            LOG.info("Finish closing writer for part "+actualPart);
        }

        private void validateSlice(Vector vector) {
            for (Vector.Element elm : vector.nonZeroes()) {
                Preconditions.checkState(
                        elm.index() % PARALLEL == actualPart,
                        "Meet index %s for part %s of total %s.",
                        elm.index(), actualPart, PARALLEL);
            }
        }

        abstract class Pzdw {
            public abstract Vector apply(int z, int d, Vector terms);
        }

        class PzdwNative extends Pzdw {
            private final double[][][] zzddww;

            public PzdwNative(int actualPart, Matrix lastPz_d, Matrix lastPw_z) {
                long ts = System.currentTimeMillis();
                int N = n_w / PARALLEL + (n_w % PARALLEL > actualPart ? 1 : 0);
                this.zzddww = new double[n_z][n_d][N];
                double [] ddzz = new double[n_d];
                double [][] wwzz = new double[N][n_z];
                for (int z=0; z<n_z; z++) {
                    for (int w=actualPart; w<n_w; w+= PARALLEL) {
                        wwzz[w/PARALLEL][z] = lastPw_z.getQuick(w, z);
                    }
                }
                for (MatrixSlice pdz : lastPz_d) {
                    int dd = pdz.index();
                    for (Vector.Element dz : pdz.nonZeroes()) {
                        ddzz[dz.index()] = dz.get();
                    }
                    for (int w=0; w<wwzz.length; w++) {
                        double zsum = 0;
                        for (int z=0; z< n_z; z++) {
                            double p = ddzz[z] * wwzz[w][z];
                            zzddww[z][dd][w] = p;
                            zsum += p;
                        }
                        if (zsum > 0) {
                            for (int z=0; z< n_z; z++) {
                                zzddww[z][dd][w] /= zsum;
                            }
                        }
                    }
                }
                LOG.info("Init native Pzdw in "+(System.currentTimeMillis() - ts)+" mills");
            }

            @Override
            public Vector apply(int z, int d, Vector terms) {
                double[] pww = zzddww[z][d];
                Vector v = new RandomAccessSparseVector(terms.size(), terms.getNumNonZeroElements());
                for (Vector.Element elm : terms.nonZeroes()) {
                    v.setQuick(elm.index(), elm.get() * pww[elm.index() / PARALLEL]);
                }
                return v;
            }
        }
    }

}
