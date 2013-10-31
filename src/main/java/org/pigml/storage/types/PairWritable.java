package org.pigml.storage.types;

import org.apache.hadoop.io.Writable;
import org.pigml.lang.Pair;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: g
 * Date: 13-9-26
 * Time: 下午4:05
 * To change this template use File | Settings | File Templates.
 */
public abstract  class PairWritable <A extends Writable, B extends Writable> extends Pair<A, B> implements Writable {

    protected PairWritable(A first, B second) {
        super(first, second);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        getFirst().write(out);
        getSecond().write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        getFirst().readFields(in);
        getSecond().readFields(in);
    }
}
