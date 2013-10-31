package org.pigml.storage.vector;

import com.google.common.base.Preconditions;
import com.twitter.elephantbird.pig.load.SequenceFileLoader;
import com.twitter.elephantbird.pig.mahout.VectorWritableConverter;
import com.twitter.elephantbird.pig.util.IntWritableConverter;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.apache.pig.data.DataType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: g
 * Date: 9/14/13
 * Time: 3:29 PM
 * To change this template use File | Settings | File Templates.
 */
public class SparseVectorLoader extends VectorLoader {

    public SparseVectorLoader() throws IOException, ParseException {
        this(null);
    }
    public SparseVectorLoader(String keyType) throws IOException, ParseException {
        super(keyType, "-sparse", null);
    }
}
