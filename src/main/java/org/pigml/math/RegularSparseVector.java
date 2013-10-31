package org.pigml.math;

import org.apache.mahout.math.AbstractVector;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.OrderedIntDoubleMapping;
import org.apache.mahout.math.Vector;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Created with IntelliJ IDEA.
 * User: g
 * Date: 13-10-14
 * Time: 上午11:14
 * To change this template use File | Settings | File Templates.
 */

//it is a sparse vector, but with value regularly populated
public class RegularSparseVector extends AbstractVector {
    private double[] elmnts;
    private final int modular;
    private final int part;
    private final int size;

    public RegularSparseVector(int size, int part, int modular) {
        super(size);
        this.modular = modular;
        this.part = part;
        this.size = size;
    }

    @Override
    protected Iterator<Element> iterator() {
        return new Iterator<Element>() {
            private int index;
            @Override
            public boolean hasNext() {
                return index < size;
            }

            @Override
            public Element next() {
                return new Element() {
                    final int fixed = index++;
                    @Override
                    public double get() {
                        if (elmnts != null) {
                            return fixed % modular == part ? elmnts[fixed/modular] : 0;
                        } else {
                            return 0;
                        }
                    }

                    @Override
                    public int index() {
                        return fixed;
                    }

                    @Override
                    public void set(double value) {
                        assureAllocated()[validate(fixed)] = value;
                    }
                };
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    protected Iterator<Element> iterateNonZero() {
        return new Iterator<Element>() {
            private int index;
            @Override
            public boolean hasNext() {
                return elmnts != null ? index < elmnts.length : false;
            }

            @Override
            public Element next() {
                final int fix = index++;
                return new Element() {
                    @Override
                    public double get() {
                        return elmnts[fix];
                    }

                    @Override
                    public int index() {
                        return fix * modular + part;
                    }

                    @Override
                    public void set(double value) {
                        elmnts[fix] = value;
                    }
                };
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    protected Matrix matrixLike(int rows, int columns) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isDense() {
        return false;
    }

    @Override
    public boolean isSequentialAccess() {
        return false;
    }

    @Override
    public void mergeUpdates(OrderedIntDoubleMapping updates) {
        throw new UnsupportedOperationException();
    }

    @Override
    public double getQuick(int index) {
        return elmnts != null ? elmnts[validate(index)] : 0;
    }

    @Override
    public Vector like() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setQuick(int index, double value) {
        assureAllocated()[validate(index)] = value;
    }

    @Override
    public int getNumNondefaultElements() {
        return size / modular + (size % modular > part ? 1 : 0);
    }

    @Override
    public double getLookupCost() {
        return 0;
    }

    @Override
    public double getIteratorAdvanceCost() {
        return 0;
    }

    @Override
    public boolean isAddConstantTime() {
        return true;
    }

    private int validate(int index) {
        if (index % modular != part) {
            throw new RuntimeException(String.format(
                    "Illegal index access (part=%d mod=%d index=%d)", part, modular, index));
        }
        return index / modular;
    }

    private double[] assureAllocated() {
        if (elmnts == null) {
            elmnts = new double[getNumNondefaultElements()];
        }
        return elmnts;
    }

    private boolean isAllocated() {
        return elmnts != null;
    }

    public void reset() {
        if (elmnts != null) {
            Arrays.fill(elmnts, 0);
        }
    }
}
