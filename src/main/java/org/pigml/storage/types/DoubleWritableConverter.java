package org.pigml.storage.types;

import com.twitter.elephantbird.pig.util.AbstractWritableConverter;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.pig.ResourceSchema;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: g
 * Date: 13-10-9
 * Time: 下午5:20
 * To change this template use File | Settings | File Templates.
 */
public class DoubleWritableConverter extends AbstractWritableConverter<DoubleWritable> {
    public DoubleWritableConverter() {
        super(new DoubleWritable());
    }

    @Override
    public ResourceSchema.ResourceFieldSchema getLoadSchema() throws IOException {
        ResourceSchema.ResourceFieldSchema schema = new ResourceSchema.ResourceFieldSchema();
        schema.setType(DataType.DOUBLE);
        return schema;
    }

    @Override
    public Object bytesToObject(DataByteArray dataByteArray) throws IOException {
        return bytesToDouble(dataByteArray.get());
    }

    @Override
    protected String toCharArray(DoubleWritable writable) throws IOException {
        return String.valueOf(writable.get());
    }

    @Override
    protected Integer toInteger(DoubleWritable writable) throws IOException {
        return (int) writable.get();
    }

    @Override
    protected Long toLong(DoubleWritable writable) throws IOException {
        return (long)writable.get();
    }

    @Override
    protected Float toFloat(DoubleWritable writable) throws IOException {
        return (float) writable.get();
    }

    @Override
    protected Double toDouble(DoubleWritable writable) throws IOException {
        return writable.get();
    }

    @Override
    public void checkStoreSchema(ResourceSchema.ResourceFieldSchema schema) throws IOException {
        switch (schema.getType()) {
            case DataType.CHARARRAY:
            case DataType.INTEGER:
            case DataType.LONG:
            case DataType.FLOAT:
            case DataType.DOUBLE:
                return;
        }
        throw new IOException("Pig type '" + DataType.findTypeName(schema.getType()) + "' unsupported");
    }

    @Override
    protected DoubleWritable toWritable(String value) throws IOException {
        return toWritable(Double.parseDouble(value));
    }

    @Override
    protected DoubleWritable toWritable(Integer value) throws IOException {
        return toWritable(value.doubleValue());
    }

    @Override
    protected DoubleWritable toWritable(Long value) throws IOException {
        return toWritable(value.doubleValue());
    }

    @Override
    protected DoubleWritable toWritable(Float value) throws IOException {
        return toWritable(value.doubleValue());
    }

    @Override
    protected DoubleWritable toWritable(Double value) throws IOException {
        writable.set(value);
        return writable;
    }
}