package org.pigml.utils;

import com.google.common.base.Preconditions;
import org.apache.mahout.math.*;
import org.apache.mahout.math.function.Functions;
import org.apache.mahout.math.function.VectorFunction;
import org.apache.mahout.math.hadoop.similarity.cooccurrence.Vectors;
import org.apache.mahout.math.map.OpenIntDoubleHashMap;

import java.util.Random;

/**
 * Created with IntelliJ IDEA.
 * User: g
 * Date: 13-10-10
 * Time: 下午2:43
 * To change this template use File | Settings | File Templates.
 */
public class MathUtils {

    static public class matrix {

        static public Matrix rand(int nr, int nc) {
            Matrix m = new DenseMatrix(nr, nc);
            Random random = new Random();
            for (int x = 0; x < nr; x++) {
                Vector row = m.viewRow(x);
                for (int term = 0; term < nc; term++) {
                    row.set(term, random.nextDouble());
                }
            }
            return m;
        }

        public static Matrix literally(int numRows, int numCols, double value) {
            double[][] elmnts = new double[numRows][numCols];
            for (int r=0; r<elmnts.length; r++) {
                for (int c=0; c < elmnts[0].length; c++) {
                    elmnts[r][c] = value;
                }
            }
            return new DenseMatrix(elmnts);
        }

        static public void normalizeRows(Matrix m) {
            for (int i=0; i<m.rowSize(); i++) {
                m.assignRow(i, m.viewRow(i).normalize(1.0));
            }
        }

        static public void normalizeColumns(Matrix m) {
            for (int i=0; i<m.columnSize(); i++) {
                m.assignColumn(i, m.viewColumn(i).normalize(1.0));
            }
        }

        static public void normalizeZ(Matrix[] matrixes) {
            Matrix zsum = sum(matrixes[0].clone(),
                    java.util.Arrays.copyOfRange(matrixes, 1, matrixes.length));
            for (Matrix m : matrixes) {
                for (int r=0; r<m.numRows(); r++) {
                    /*for (int c=0; c<m.columnSize(); c++) {
                        double v = m.getQuick(r, c);
                        if (v > 0) {
                            m.setQuick(r, c, v / zsum.getQuick(r, c));
                        }
                    }*/
                    Vector row = m.viewRow(r);
                    for (Vector.Element col : row.nonZeroes()) {
                        row.setQuick(col.index(), col.get() / zsum.getQuick(r, col.index()));
                    }
                }
            }
        }

        static public Matrix zsum(Matrix[] matrixes) {
            return sum(matrixes[0].clone(),
                    java.util.Arrays.copyOfRange(matrixes, 1, matrixes.length));
        }

        static public Vector sumRow(Matrix m) {
            return m.aggregateRows(ZSUM);
        }

        static public Vector sumCol(Matrix m) {
            return m.aggregateColumns(ZSUM);
        }

        //m0 will be changed
        static public Matrix sum(Matrix m0, Matrix... ms) {
            for (Matrix m : ms) {
                if (m != null) {
                    m0.assign(m, Functions.PLUS);
                }
            }
            return m0;
        }
    }

    static public class vector {
        static public int[] keys(Vector v) {
            int[] result = new int[v.getNumNonZeroElements()];
            int k = 0;
            for (Vector.Element elm : v.nonZeroes()) {
                result[k++] = elm.index();
            }
            return result;
        }

        static public Vector multiply(Vector A, Vector B) {
            return A.clone().assign(B, Functions.MULT); //TBD performance
        }

        static public Vector add(Vector A, Vector B) {
            return A.clone().assign(B, Functions.PLUS); //TBD performance
        }

        static public Vector literally(int dim, double first, double... remainings) {
            Preconditions.checkArgument(dim > remainings.length);
            double[] values = new double[dim];
            int j = 0;
            values[j++] = first;
            for (double d : remainings) {
                values[j++] = d;
            }
            while (j<dim) {
                values[j] = values[j-1];
                j++;
            }
            return new DenseVector(values);
        }

    }

    static public final VectorFunction ZSUM = new VectorFunction() {
        @Override
        public double apply(Vector f) {
            return f.zSum();
        }
    };

    static public Matrix asMatrix(Vector v) {
        Matrix m = new SparseMatrix(1, v.size());
        m.assignRow(0, v);
        return m;
    }
}
