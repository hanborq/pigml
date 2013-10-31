package org.pigml.udf.vector;

import org.apache.mahout.math.Vector;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.pigml.utils.VectorConverter;

import java.io.IOException;

/**
 * Sum all elements of a vector.
 */
public class VectorZSum extends EvalFunc<Double>{

    private VectorConverter.VectorType vectorType;


    @Override
    public Double exec(Tuple input) throws IOException {
        Tuple tuple = (Tuple) input.get(0);
        if (vectorType == null) {
            // we only infer vector type from first input
            // and assume that the following input has the same type
            vectorType = VectorConverter.getVectorTypeOfTuple(tuple);
        }

        Vector vector = VectorConverter.convertToVector(tuple, vectorType);
        return vector.zSum();
    }

    @Override
    public Schema outputSchema(Schema input) {
        if (input.size() != 1) {
            throw new RuntimeException(
                    "Expect (tuple), input " + input.size() + " fields");
        }

        try {
            Schema vectorSchema = input.getField(0).schema;
            VectorConverter.checkVectorSchema(vectorSchema);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return new Schema(new Schema.FieldSchema(null, DataType.DOUBLE));
    }

}
