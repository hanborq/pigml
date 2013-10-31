package org.pigml.cluster.plsi.asym;

import com.google.common.base.Preconditions;
import com.google.common.collect.Ordering;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.MatrixSlice;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.UDFContext;
import org.pigml.lang.Pair;
import org.pigml.utils.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: g
 * Date: 9/14/13
 * Time: 2:55 PM
 * To change this template use File | Settings | File Templates.
 */

public class PLSIPzdw extends EvalFunc<DataBag> {
    private final String location;
    private VectorUtils.Tuple2Elements elgen;
    private Ordering<Pair<Integer,Double>> ordering;
    private TupleFactory tfac;
    private BagFactory bfac;

    private VectorUtils.Tuple2Vector vgen;
    private int PARALLEL = 0;

    private int n_w;
    private int n_z, n_d;

    public PLSIPzdw(String location, String nz, String nd, String nw, String parallel) throws IOException {
        this.location = location;
        this.n_d = Integer.valueOf(nd); //TODO figure these out from job conf
        this.n_w = Integer.valueOf(nw);
        this.n_z = Integer.valueOf(nz);
        this.PARALLEL = Integer.valueOf(parallel);

        Env.inBackground(new Env.BackgroundProcedure() {
            @Override
            public void execute(Configuration conf) throws IOException {
                vgen = VectorUtils.createVectorGenerator(
                        Env.getProperty(PLSIPzdw.class, "vector.type"));
            }
        });
    }

    @Override
    public DataBag exec(Tuple input) throws IOException {
        if (input == null) {
            return null;
        }
        int d = (Integer)input.get(0);
        Vector terms = vgen.apply((Tuple) input.get(1));
        if (initBackground(terms)) {
            return pzdw.apply(d, terms);
        }
        return null;
    }

    @Override
    public Schema outputSchema(final Schema input) {
        SchemaUtils.claim(input, 0, DataType.INTEGER);
        Schema.FieldSchema tuple = SchemaUtils.claim(input, 0, DataType.TUPLE);
        Env.setProperty(PLSIPzdw.class, "vector.type", VectorUtils.typeOfVector(tuple.schema));

        List<Schema.FieldSchema> fields = new ArrayList(2 + n_z);
        fields.add(SchemaUtils.primitiveOf(DataType.INTEGER));
        fields.add(SchemaUtils.primitiveOf(DataType.INTEGER));
        for (int z=0; z<n_z; z++) {
            fields.add(SchemaUtils.primitiveOf(DataType.DOUBLE));
        }
        return SchemaUtils.schemaOf(SchemaUtils.bagOf(fields.toArray(new Schema.FieldSchema[0])));
    }

    private boolean initBackground(Vector terms) throws IOException {
        if (terms.getNumNonZeroElements() == 0) {
            return false;
        }
        int actualPart = terms.nonZeroes().iterator().next().index() % PARALLEL;
        if (pzdw != null && pzdw.actualPart == actualPart) {
            return true;
        }
        Configuration conf = UDFContext.getUDFContext().getJobConf();
        FileSystem fs = FileSystem.get(conf);
        Path savedAt = new Path(location);
        Matrix lastPz_d, lastPw_z;
        if (fs.exists(PathUtils.enter(savedAt, "Pz_d"))) {
            LOG.info("Loading model from previous iteration");
            lastPz_d = IOUtils.loadMatrix(conf, PathUtils.enter(savedAt, "Pz_d")); //TODO...
            lastPw_z = IOUtils.loadMatrix(conf, PathUtils.enter(savedAt, "Pw_z"));
        } else {
            LOG.info("Generating model for first run");
            lastPz_d = MathUtils.matrix.rand(n_d, n_z);
            lastPw_z = MathUtils.matrix.rand(n_w, n_z);
        }
        pzdw = new PzdwNative(actualPart, lastPz_d, lastPw_z);
        return true;
    }

    PzdwNative pzdw;

    class PzdwNative {
        private final double[][] pdw;
        private double[][] pzd;
        private double[][] pwz;
        private final int actualPart;

        public PzdwNative(int actualPart, Matrix lastPz_d, Matrix lastPw_z) {
            this.actualPart = actualPart;
            long ts = System.currentTimeMillis();
            int N = n_w / PARALLEL + (n_w % PARALLEL > actualPart ? 1 : 0);
            this.pzd = new double[n_d][n_z];
            this.pwz = new double[N][n_z];
            this.pdw = new double[n_d][N];
            for (int z=0; z<n_z; z++) {
                for (int w=actualPart; w<n_w; w+= PARALLEL) {
                    pwz[w/PARALLEL][z] = lastPw_z.getQuick(w, z);
                }
                for (int d=0; d<n_d; d++) {
                    pzd[d][z] = lastPz_d.getQuick(d, z);
                }
            }
            for (int d=0; d<n_d; d++) {
                for (int w=actualPart; w<n_w; w+= PARALLEL) {
                    for (int z=0; z<n_z; z++) {
                        pdw[d][w/PARALLEL] += pzd[d][z] * pwz[w/PARALLEL][z];
                    }
                }
            }
            LOG.info("Init native Pzdw in "+(System.currentTimeMillis() - ts)+" mills");
        }

        // {(d,w,p...), (d,w,p...)}
        public DataBag apply(int d, Vector terms) throws ExecException {
            List<Tuple> tuples = new ArrayList<Tuple>(terms.getNumNonZeroElements());
            for (Vector.Element elm : terms.nonZeroes()) {
                int w = elm.index();
                Tuple inner = tfac.newTuple(2 + n_z);
                inner.set(0, d);
                inner.set(1, w);
                for (int z=0; z<n_z; z++) {
                    Preconditions.checkState(pdw[d][w] != 0);
                    inner.set(z+2, pwz[w][z] * pzd[d][z] * elm.get() / pdw[d][w]);
                }
                tuples.add(inner);
            }
            return bfac.newDefaultBag(tuples);
        }
    }
    private static final Log LOG = LogFactory.getLog(PLSIPzdw.class);
}
