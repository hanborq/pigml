package org.pigml.storage.writable;

import com.google.common.collect.Lists;
import com.twitter.elephantbird.mapreduce.input.RawSequenceFileInputFormat;
import com.twitter.elephantbird.pig.load.LzoBaseLoadFunc;
import com.twitter.elephantbird.util.HadoopCompat;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.pig.*;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DefaultTuple;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.ObjectSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;

/**
 * Created with IntelliJ IDEA.
 * User: g
 * Date: 13-9-26
 * Time: 下午4:46
 * To change this template use File | Settings | File Templates.
 */
public class PigLoader extends LoadFunc {

    private static final Logger LOG = LoggerFactory.getLogger(PigLoader.class);

    @SuppressWarnings("rawtypes")
    protected RecordReader reader;

    @Override
    public void setLocation(String location, Job job) throws IOException {
        FileInputFormat.setInputPaths(job, location);
    }

    @Override
    public InputFormat getInputFormat() throws IOException {
        return new RawSequenceFileInputFormat();
    }

    @Override
    public LoadCaster getLoadCaster() throws IOException {
        return null;
    }

    @Override
    public void prepareToRead(@SuppressWarnings("rawtypes") RecordReader reader, PigSplit split) {
        this.reader = reader;
    }

    @Override
    public Tuple getNext() throws IOException {
        try {
            if (!reader.nextKeyValue()) {
                return null;
            }
            DataInputBuffer ibuf = (DataInputBuffer) reader.getCurrentValue();
            Tuple t = new DefaultTuple();
            t.readFields(ibuf);
            return t;
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }
}
