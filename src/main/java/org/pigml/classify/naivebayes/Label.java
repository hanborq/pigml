package org.pigml.classify.naivebayes;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.io.IOException;

/**
 * Extract label.
 */
public class Label extends EvalFunc<String> {

    @Override
    public String exec(Tuple input) throws IOException {
        String field = (String) input.get(0);
        return field.split("/")[1];
    }

    @Override
    public Schema outputSchema(Schema input) {
        if (input.size() != 1) {
            throw new RuntimeException(
                    "Expected (chararray), input " + input.size() + " fields");
        }

        try {
            Schema.FieldSchema firstField = input.getField(0);
            if (firstField.type != DataType.CHARARRAY) {
                String msg = "Expected input (chararray, ...), received schema (";
                msg += DataType.findTypeName(firstField.type);
                throw new RuntimeException(msg);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY));
    }
}
