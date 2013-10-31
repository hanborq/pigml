package org.pigml.storage.types;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * Created with IntelliJ IDEA.
 * User: g
 * Date: 13-9-26
 * Time: 下午4:13
 * To change this template use File | Settings | File Templates.
 */
public class StringIntPair extends PairWritable<Text, IntWritable> {

    public StringIntPair() {
        super(new Text(), new IntWritable());
    }
}
