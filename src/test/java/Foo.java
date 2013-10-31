import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.SparseMatrix;
import org.apache.mahout.math.Vector;
import org.pigml.utils.IOUtils;
import org.pigml.utils.MathUtils;
import org.pigml.utils.PathUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: g
 * Date: 13-10-13
 * Time: 下午8:56
 * To change this template use File | Settings | File Templates.
 */
public class Foo {
    private static final String DEFAULT_PART = "part-00000";

    static public void main(String[] args) throws IOException {
        testIOUtils(new Runner());
    }

    static public void testIOUtils(Runner runner) throws IOException {
        final Matrix m1 = MathUtils.matrix.literally(3, 4, 1);
        final Matrix m2 = MathUtils.matrix.literally(3, 4, 2);
        final Matrix m3 = MathUtils.matrix.literally(3, 4, 3);
        final Matrix m6 = MathUtils.matrix.literally(3, 4, 6);


        runner.run(new Case("test load save...") {
            @Override
            public boolean run() throws IOException {
                final Configuration conf = new Configuration();
                final Path pm1 = new Path("/tmp/foo/m1");
                final Path pm2 = new Path("/tmp/foo/m2");
                final Path pm3 = new Path("/tmp/foo/m3");
                IOUtils.saveMatrix(pm1, conf, m1);
                IOUtils.saveMatrix(pm2, conf, m2);
                IOUtils.saveMatrix(pm3, conf, m3);
                Matrix merge = null;
                for (Path path : new Path[]{pm1, pm2, pm3}) {
                    merge = MathUtils.matrix.sum(IOUtils.loadMatrix(conf, path), merge);
                }
                return Foo.equals(merge, m6);
            }
        });

        runner.run(new Case("Test Sum") {
            @Override
            public boolean run() throws IOException {
                Matrix ms = MathUtils.matrix.sum(m1, m2);
                return Foo.equals(ms, m3) && m1 == ms;
            }
        });

        runner.run(new Case("Test Normalize Row") {
            @Override
            public boolean run() throws IOException {
                MathUtils.matrix.normalizeRows(m1);
                double delta = 0;
                for (int r=0; r<m1.numRows(); r++) {
                    delta = Math.max(delta, Math.abs(1.0 - m1.viewRow(r).zSum()));
                }
                return delta < 0.001;
            }
        });

        runner.run(new Case("Test Normalize Row") {
            @Override
            public boolean run() throws IOException {
                Matrix m = MathUtils.matrix.rand(3,4);
                MathUtils.matrix.normalizeRows(m);
                double delta = 0;
                for (int r=0; r<m.numRows(); r++) {
                    delta = Math.max(delta, Math.abs(1.0 - m.viewRow(r).zSum()));
                }
                return delta < 0.001;
            }
        });

        runner.run(new Case("Test Normalize Col") {
            @Override
            public boolean run() throws IOException {
                Matrix m = MathUtils.matrix.rand(3,4);
                MathUtils.matrix.normalizeColumns(m);
                double delta = 0;
                for (int r=0; r<m.numCols(); r++) {
                    delta = Math.max(delta, Math.abs(1.0 - m.viewColumn(r).zSum()));
                }
                return delta < 0.001;
            }
        });

        println("DONE");
    }

    abstract static class Case {
        private final String name;

        public Case(String name) {
            this.name = name;
        }
        abstract public boolean run() throws IOException;
    }

    static class Runner {
        void run(Case c) throws IOException {
            println("Running "+c.name);
            if (!c.run()) {
                new Exception("Test fail stack").printStackTrace();
            } else {
                println("ok");
            }
        }
    }

    static private <T> void println(T t) {
        System.out.println(t);
    }

    static private <T> void eprintln(T t) {
        System.err.println(t);
    }

    static boolean equals(Matrix m1, Matrix m2) {
        if (m1.numRows() != m2.numRows() || m1.numCols() != m2.numCols())
            return false;
        for (int r = 0; r < m1.numRows(); r++) {
            for (int c =0; c<m1.numCols(); c++) {
                if (m1.getQuick(r, c) != m2.getQuick(r, c)) {
                    println("mismatch at "+r+","+c+"="+m1.getQuick(r,c)+"/"+m2.getQuick(r,c));
                    return false;
                }
            }
        }
        return true;
    }

    static public void testMahoutMatrix(String[] args) throws IOException {
        int nz = 5;
        int nd = 21670;
        int nw = 41900/200;
        int ITER = 20;
        for (int iter=0; iter<ITER; iter++) {
            System.out.printf("iterating for %d\n", iter);
            System.out.printf("allocating pzdw\n");
            long ts = System.currentTimeMillis();
            Matrix[] pzdw = new Matrix[nz];
            for (int z=0; z<pzdw.length; z++) {
                pzdw[z] = new SparseMatrix(nd, nw);
            }
            ts = System.currentTimeMillis() - ts;
            System.out.printf("  Finish in %dms\n", ts);

            double rand = Math.random();
            System.out.printf("populating pzdw with rand %f\n", rand);
            ts = System.currentTimeMillis();
            for (Matrix ddd : pzdw) {
                for (int d=0; d<nd; d++) {
                    for (int w=0; w<nw; w++) {
                        ddd.setQuick(d, w, rand);
                    }
                }
            }
            ts = System.currentTimeMillis() - ts;
            System.out.printf("  Finish in %dms\n", ts);

            System.out.printf("sum on pzdw\n");
            double sum = 0;
            ts = System.currentTimeMillis();
            for (Matrix ddd : pzdw) {
                for (int d=0; d<nd; d++) {
                    for (int w=0; w<nw; w++) {
                        sum += ddd.getQuick(d, w);
                    }
                }
            }
            ts = System.currentTimeMillis() - ts;
            System.out.printf("  Finish in %dms\n", ts);
            System.out.printf("  Sum is %f\n", sum);

            System.out.println();
        }
    }

    static public void testNativeMatrix(String[] args) throws IOException {
        int nz = 5;
        int nd = 21670;
        int nw = 41900/200;
        int ITER = 20;
        for (int iter=0; iter<ITER; iter++) {
            System.out.printf("iterating for %d\n", iter);
            System.out.printf("allocating pzdw\n");
            long ts = System.currentTimeMillis();
            double[][][] pzdw = new double[nz][nd][nw];
            ts = System.currentTimeMillis() - ts;
            System.out.printf("  Finish in %dms\n", ts);

            double rand = Math.random();
            System.out.printf("populating pzdw with rand %f\n", rand);
            ts = System.currentTimeMillis();
            for (double[][] ddd : pzdw) {
                for (double[] dd : ddd) {
                    for (int i=0; i<dd.length; i++) {
                        dd[i] = rand;
                    }
                }
            }
            ts = System.currentTimeMillis() - ts;
            System.out.printf("  Finish in %dms\n", ts);

            System.out.printf("sum on pzdw\n");
            double sum = 0;
            ts = System.currentTimeMillis();
            for (double[][] ddd : pzdw) {
                for (double[] dd : ddd) {
                    for (int i=0; i<dd.length; i++) {
                        sum += dd[i];
                    }
                }
            }
            ts = System.currentTimeMillis() - ts;
            System.out.printf("  Finish in %dms\n", ts);
            System.out.printf("  Sum is %f\n", sum);

            System.out.println();
        }
    }

    static public void main0(String[] args) throws IOException {
        Configuration conf = new Configuration();
        Path savedAt = new Path("/home/hadoop/hthome/pig-0.11.1/plsi");
        Map<String, Matrix> mm = new HashMap<String, Matrix>();
        System.out.printf("Loading Pz...\n");
        mm.put("Pz", IOUtils.loadMatrix(
                conf, PathUtils.enter(savedAt, "Pz", DEFAULT_PART)));

        System.out.printf("Loading Pd_z...\n");
        mm.put("Pd_z", IOUtils.loadMatrix(conf, PathUtils.enter(savedAt, "Pd_z", DEFAULT_PART)));

        System.out.printf("Loading Pw_z...\n");
        mm.put("Pw_z", IOUtils.loadMatrix(conf, PathUtils.enter(savedAt, "Pw_z", DEFAULT_PART)));

        for (Map.Entry<String, Matrix> ent : mm.entrySet()) {
            Matrix mat = ent.getValue();
            System.out.printf("Validating %s of %d x %d = %f...\n",
                    ent.getKey(), mat.numRows(), mat.numCols(), mat.zSum());
            for (Vector v : mat) {
                if (Double.isNaN(v.zSum())) {
                    System.out.printf("___ failed validate %s\n", v);
                }
            }
        }
        System.out.println("DONE");
    }
}
