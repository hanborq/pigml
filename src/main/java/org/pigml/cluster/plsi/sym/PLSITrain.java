package org.pigml.cluster.plsi.sym;

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
import org.apache.mahout.math.function.VectorFunction;
import org.apache.pig.ResourceSchema;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.UDFContext;
import org.pigml.math.RegularSparseVector;
import org.pigml.storage.AbstractStore;
import org.pigml.storage.types.NullOutputFormat;
import org.pigml.utils.*;

import java.io.IOException;
import java.util.Random;

/**
 * Thomas Hofmann: Probabilistic Latent Semantic Analysis
 *   Proc. of the Twenty-Second Annual International SIGIR Conf. on Research
 *   and Development in Information Retrieval
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
        Vector lastPz;
        Matrix lastPd_z, lastPw_z;
        if (fs.exists(PathUtils.enter(savedAt, "Pz", DEFAULT_PART))) {
            LOG.info("Loading model from previous iteration");
            lastPz = IOUtils.loadMatrix(
                    conf, PathUtils.enter(savedAt, "Pz", DEFAULT_PART)).viewRow(0);
            lastPd_z = IOUtils.loadMatrix(conf, PathUtils.enter(savedAt, "Pd_z", DEFAULT_PART));
            lastPw_z = IOUtils.loadMatrix(conf, PathUtils.enter(savedAt, "Pw_z", DEFAULT_PART));
        } else {
            LOG.info("Generating model for first run");
            lastPz = MathUtils.vector.literally(n_z, 1.0 / n_z);
            lastPd_z = MathUtils.matrix.rand(n_d, n_z);
            lastPw_z = MathUtils.matrix.rand(n_w, n_z);
            MathUtils.matrix.normalizeColumns(lastPd_z);
            MathUtils.matrix.normalizeColumns(lastPw_z);
        }
        trainer = new PartTrainer(actualPart, lastPz, lastPd_z, lastPw_z);
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
        final Path storeLocation = new Path(location);
        final Path loadFrom = PathUtils.enter(storeLocation, "."+iter);
        final Path saveTo = PathUtils.enter(storeLocation);

        //TODO single point operation. rearchitect for parallel

        LOG.info("Merging, normalizing, saving Pw_z");

        Matrix merge = null;
        for (Path path : PathUtils.listPath(fs, PathUtils.enter(loadFrom, "Pw_z"))) {
            merge = MathUtils.matrix.sum(IOUtils.loadMatrix(conf, path), merge);
        }
        Preconditions.checkState(merge.rowSize() == n_w && merge.columnSize() == n_z);
        MathUtils.matrix.normalizeColumns(merge);
        IOUtils.saveMatrix(PathUtils.enter(saveTo, "Pw_z", DEFAULT_PART), conf, merge);

        LOG.info("Merging, normalizing, saving Pd_z");

        merge = null;
        for (Path path : PathUtils.listPath(fs, PathUtils.enter(loadFrom, "Pd_z"))) {
            merge = MathUtils.matrix.sum(IOUtils.loadMatrix(conf, path), merge);
        }
        Preconditions.checkState(merge.rowSize() == n_d && merge.columnSize() == n_z);
        MathUtils.matrix.normalizeColumns(merge);
        IOUtils.saveMatrix(PathUtils.enter(saveTo, "Pd_z", DEFAULT_PART), conf, merge);

        LOG.info("Merging, normalizing, saving Pz");

        merge = null;
        for (Path path : PathUtils.listPath(fs, PathUtils.enter(loadFrom, "Pz"))) {
            merge = MathUtils.matrix.sum(IOUtils.loadMatrix(conf, path), merge);
        }
        Preconditions.checkState(merge.rowSize() == 1 && merge.columnSize() == n_z);
        MathUtils.matrix.normalizeRows(merge);
        IOUtils.saveMatrix(PathUtils.enter(saveTo, "Pz", DEFAULT_PART), conf, merge);

        IterationUtils.commitIteration(conf, location, iter);

        LOG.info("FINISHED iteration " + iter + " at " + storeLocation);
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
        private Matrix Pd_z; //dense
        private Matrix Pw_z; //dense
        private final int actualPart;
        private PzdwNative Pz_dw;

        public PartTrainer(int actualPart, Vector lastPz, Matrix lastPd_z, Matrix lastPw_z) {
            /**
             * Calcuate Pz_dw per Formula-5, Hofmann 99.
             *
             * About slice:
             *   Slice in the space of words is to reduce memory usage which is necessary.
             *   The input documents should be sliced the same way as model 'Pw_z'.
             */
            /*boolean slow = Env.getJobDefine(
                    UDFContext.getUDFContext().getJobConf(), "plsi.slow", Functors.AS_BOOLEAN, "false");*/
            this.actualPart = actualPart;
            this.Pz_dw = new PzdwNative(actualPart, lastPz, lastPd_z, lastPw_z);
            Pw_z = new SparseRowMatrix(n_w, n_z);
            for (int w = actualPart; w < n_w; w+=PARALLEL) {
                Pw_z.assignRow(w, MathUtils.vector.literally(n_z, 0));
            }
            Pd_z = MathUtils.matrix.literally(n_d, n_z, 0);
            LOG.info("Finished pre-mstep for part "+actualPart);
        }

        public void process (int d, Vector terms) {
            validateSlice(terms);
            for (int z=0; z<n_z; z++) {
                Vector pzdw = Pz_dw.apply(z, d, terms);
                for (Vector.Element we : pzdw.nonZeroes()) {
                    int w = we.index();
                    Pw_z.setQuick(w, z, Pw_z.getQuick(w, z) + we.get());
                }
                //Pw_z.viewColumn(z).assign(pzdw, Functions.PLUS); //Formula-6 un-normalized
                Preconditions.checkState(Pd_z.getQuick(d, z) == 0);
                Pd_z.setQuick(d, z, pzdw.zSum());                  //Formula-7 un-normalized
                //Pz.set(z, Pz.get(z) + pzdwsum);                //Formula-8 un-normalized
            }
        }

        public void close(Configuration conf) throws IOException {
            LOG.info("Closing writer for part "+actualPart);
            int iter = Integer.parseInt(Env.getProperty(PLSITrain.class, "allocated.iteration"));
            Path saveTo = PathUtils.enter(getStorePath(), "." + iter);
            IOUtils.saveMatrix(PathUtils.enter(
                    saveTo, "Pd_z", String.valueOf(actualPart)), conf, Pd_z);
            IOUtils.saveMatrix(PathUtils.enter(
                    saveTo, "Pw_z", String.valueOf(actualPart)), conf, Pw_z);
            Vector pz = Pd_z.aggregateColumns(new VectorFunction() {
                @Override
                public double apply(Vector f) {
                    return f.zSum();
                }
            });
            Preconditions.checkState(pz.size() == n_z);
            IOUtils.saveMatrix(PathUtils.enter(
                    saveTo, "Pz", String.valueOf(actualPart)), conf, MathUtils.asMatrix(pz));
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

            public PzdwNative(int actualPart, Vector lastPz, Matrix lastPd_z, Matrix lastPw_z) {
                long ts = System.currentTimeMillis();
                int N = n_w / PARALLEL + (n_w % PARALLEL > actualPart ? 1 : 0);
                this.zzddww = new double[n_z][n_d][N];
                double[] pz = new double[n_z];
                double [][] wwzz = new double[N][n_z];
                for (int z=0; z<n_z; z++) {
                    pz[z] = lastPz.getQuick(z);
                    for (int w=actualPart; w<n_w; w+= PARALLEL) {
                        wwzz[w/PARALLEL][z] = lastPw_z.getQuick(w, z);
                    }
                }
                for (int dd=0; dd<n_d; dd++) {
                    double [] ddzz = new double[n_z];
                    for (int z=0; z<n_z; z++) {
                        ddzz[z] = lastPd_z.getQuick(dd, z);
                    }
                    for (int w=0; w<wwzz.length; w++) {
                        double zsum = 0;
                        for (int z=0; z< n_z; z++) {
                            double p = pz[z] * ddzz[z] * wwzz[w][z];
                            zzddww[z][dd][w] = p;
                            zsum += p;
                        }
                        if (zsum > 0) {
                            for (int z=0; z< n_z; z++) {
                                zzddww[z][dd][w] = zzddww[z][dd][w] / zsum;
                            }
                        }
                    }
                }
                LOG.info("Init native Pzdw in "+(System.currentTimeMillis() - ts)+" mills");
            }

            @Override
            public Vector apply(int z, int d, Vector terms) {
                double[] ppw = zzddww[z][d];
                Vector v = new RandomAccessSparseVector(terms.size(), terms.getNumNonZeroElements());
                for (Vector.Element elm : terms.nonZeroes()) {
                    v.setQuick(elm.index(), elm.get() * ppw[elm.index() / PARALLEL]);
                }
                return v;
            }
        }

        class PzdwMatrix extends Pzdw {
            private final Matrix[] Pz_dw;

            public PzdwMatrix(int actualPart, Vector lastPz, Matrix lastPd_z, Matrix lastPw_z) {
                long ts = System.currentTimeMillis();
                Pz_dw = new Matrix[n_z];
                for (int z=0; z< Pz_dw.length; z++) {
                    //Pz_dw[z] = new SparseMatrix(n_d, n_w);
                    Vector[] rows = new Vector[n_d];
                    for (int i=0; i<n_d; i++) {
                        rows[i] = new RegularSparseVector(n_w, actualPart, PARALLEL);
                    }
                    Pz_dw[z] = new SparseRowMatrix(n_d, n_w, rows, true, true);
                }
                long zero = 0;
                long total = 0;
                for (MatrixSlice pdz : lastPd_z) {
                    Preconditions.checkState(pdz.index() < n_d);
                    for (MatrixSlice pwz : lastPw_z) {
                        if (pwz.index() % PARALLEL == actualPart) { //THIS DO THE WORD SLICE
                            Preconditions.checkState(pwz.index() < n_w);
                            int dd = pdz.index();
                            int ww = pwz.index();
                            double zsum = 0;
                            for (int z=0; z< Pz_dw.length; z++) {
                                double p = lastPz.get(z) * pdz.getQuick(z) * pwz.getQuick(z);
                                Preconditions.checkState(!Double.isNaN(p));
                                Pz_dw[z].setQuick(dd, ww, p);
                                zsum += p;
                            }
                            total++;
                            if (zsum > 0) {
                                for (int z=0; z< Pz_dw.length; z++) {
                                    Pz_dw[z].setQuick(dd, ww,
                                            Pz_dw[z].getQuick(dd, ww) / zsum);
                                }
                            } else {
                                zero++;
                            }
                        }
                    }
                }
                LOG.info("Init matrix based Pzdw in "+(System.currentTimeMillis() - ts)+" mills");
            }

            @Override
            public Vector apply(int z, int d, Vector terms) {
                return MathUtils.vector.multiply(Pz_dw[z].viewRow(d), terms);
            }
        }
    }

}
