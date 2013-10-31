package org.pigml.udf.vector;

import org.apache.mahout.math.Vector;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.pigml.utils.VectorConverter;

import java.io.IOException;

/**
 * Divide a Vector with a scalar.
 */
public class VectorMinus extends EvalFunc<Tuple> {

    private VectorConverter.VectorType vectorType;

    @Override
    public Tuple exec(Tuple input) throws IOException {
        Tuple vectorTuple = (Tuple) input.get(0);
        Number scalar = (Number) input.get(1);
        if (vectorType == null) {
            // infer vector type only on first input
            vectorType = VectorConverter.getVectorTypeOfTuple(vectorTuple);
        }

        Vector vector = VectorConverter.convertToVector(vectorTuple, vectorType);
        Vector result = vector.clone();
        result.plus(-scalar.doubleValue());
        return VectorConverter.convertToTuple(result, vectorType);
    }

    @Override
    public Schema outputSchema(Schema input) {
        if (input.size() != 2) {
            throw new RuntimeException(
                    "Expect (tuple, number), input " + input.size() + " fields");
        }
        Schema vectorSchema;
        try {
            vectorSchema = input.getField(0).schema;
            VectorConverter.checkVectorSchema(vectorSchema);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        try {
            if (!DataType.isNumberType(input.getField(1).type)) {
                throw new RuntimeException(
                        "Expect (tuple, number), second field schema of input is " +
                                DataType.findTypeName(input.getField(1).type));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return vectorSchema;
    }
}
