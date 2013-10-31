package org.pigml.classify.naivebayes;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.pigml.utils.SchemaUtils;

import java.io.IOException;
import java.util.*;

/**
 * Assign an ID to each label.
 */
public class IndexLabels extends EvalFunc<DataBag> {

    @Override
    public DataBag exec(Tuple input) throws IOException {
        DataBag labels = (DataBag) input.get(0);
        SortedSet<String> labelSet = new TreeSet<String>();
        for (Tuple tuple : labels) {
            labelSet.add((String) tuple.get(0));
        }

        BagFactory bagFactory = BagFactory.getInstance();
        TupleFactory tupleFactory = TupleFactory.getInstance();

        DataBag labelIndex = bagFactory.newDefaultBag();
        int index = 0;
        for (String label : labelSet) {
            labelIndex.add(tupleFactory.newTupleNoCopy(
                    ImmutableList.of(label, index++)));
        }
        return labelIndex;
    }

    @Override
    public Schema outputSchema(Schema input) {
        Preconditions.checkArgument(input.size() == 1,
                "Expect ({chararray}), input " + input.size() + " fields.");

        try {
            Schema labelsSchema = input.getField(0).schema.getField(0).schema;
            SchemaUtils.claim(labelsSchema, 0, DataType.CHARARRAY);
        } catch (FrontendException e) {
            throw new RuntimeException(e);
        }

        return new Schema(SchemaUtils.bagOf(
                new Schema.FieldSchema("label", DataType.CHARARRAY),
                new Schema.FieldSchema("index", DataType.INTEGER)));
    }
}
