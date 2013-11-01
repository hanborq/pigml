package org.pigml.storage.types;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.twitter.elephantbird.pig.util.AbstractWritableConverter;
import org.apache.mahout.common.StringTuple;
import org.apache.pig.ResourceSchema;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.pigml.utils.SchemaUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: g
 * Date: 13-9-26
 * Time: 下午2:13
 * To change this template use File | Settings | File Templates.
 */
public class StringTupleConverter extends AbstractWritableConverter<StringTuple> {
    private BagFactory bfac = DefaultBagFactory.getInstance();
    private TupleFactory tfac = TupleFactory.getInstance();

    public StringTupleConverter() {
        super(new StringTuple());
    }

    @Override
    public ResourceSchema.ResourceFieldSchema getLoadSchema() throws IOException {
        Schema tupleSchema = new Schema(Lists.newArrayList(new Schema.FieldSchema(null,
                DataType.CHARARRAY)));
        Schema bagSchema = new Schema(Lists.newArrayList(new Schema.FieldSchema("t", tupleSchema,
                DataType.TUPLE)));
        return new ResourceSchema.ResourceFieldSchema(
                new Schema.FieldSchema("entries", bagSchema, DataType.BAG));
    }

    @Override
    public Object bytesToObject(DataByteArray dataByteArray) throws IOException {
        return bytesToBag(dataByteArray.get(), null);
    }

    @Override
    protected DataBag toBag(StringTuple writable, ResourceSchema.ResourceFieldSchema schema) throws IOException {
        Preconditions.checkNotNull(writable, "StringTuple is null");
        List<Tuple> tuples = new ArrayList<Tuple>(writable.length());
        for (String s : writable.getEntries()) {
            tuples.add(tfac.newTuple(s));
        }
        return bfac.newDefaultBag(tuples);
    }

    @Override
    public void checkStoreSchema(ResourceSchema.ResourceFieldSchema schema) throws IOException {
        ResourceSchema.ResourceFieldSchema bag = schema;//SchemaUtils.claim(schema, 0, DataType.BAG);
        ResourceSchema.ResourceFieldSchema tuple = SchemaUtils.claim(bag, 0, DataType.TUPLE);
        SchemaUtils.claim(tuple, 0, DataType.CHARARRAY);
    }

    /*@Override
    protected StringTuple toWritable(Tuple value) throws IOException {
        if (value == null) {
            return null;
        }
        StringTuple st = new StringTuple();
        DataBag bag = (DataBag) value.get(0);
        for (Tuple t : bag) {
            st.add((String) t.get(0));
        }
        return st;
    }*/

    @Override
    protected StringTuple toWritable(DataBag bag) throws IOException {
        StringTuple st = new StringTuple();
        for (Tuple t : bag) {
            st.add((String) t.get(0));
        }
        return st;
    }
}
