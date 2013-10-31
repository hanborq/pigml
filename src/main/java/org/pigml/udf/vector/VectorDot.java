package org.pigml.udf.vector;

import com.google.common.base.Preconditions;
import org.apache.mahout.math.Vector;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.pigml.utils.VectorConverter;

import java.io.IOException;

import static org.pigml.utils.VectorConverter.VectorType;

/**
 * Compute dot product of two vectors.
 */
public class VectorDot extends EvalFunc<Tuple> {
    private static VectorType leftType;
    private static VectorType rightType;

    @Override
    public Tuple exec(Tuple input) throws IOException {
        Tuple leftTuple = (Tuple) input.get(0);
        Preconditions.checkNotNull(leftTuple, "left vector operand is null");
        Tuple rightTuple = (Tuple) input.get(1);
        Preconditions.checkNotNull(rightTuple, "right vector operand is null");

        if (leftType == null) {
            leftType = VectorConverter.getVectorTypeOfTuple(leftTuple);
        }
        if (rightType == null) {
            rightType = VectorConverter.getVectorTypeOfTuple(rightTuple);
        }

        Vector left = VectorConverter.convertToVector(leftTuple, leftType);
        Vector right = VectorConverter.convertToVector(rightTuple, rightType);
        Preconditions.checkArgument(left.size() == right.size(),
                "vectors require the same cardinality");

        Vector result = left.clone();
        result.dot(right);
        return VectorConverter.convertToTuple(result, leftType);
    }

    @Override
    public Schema outputSchema(Schema input) {
        if (input.size() != 2) {
            throw new RuntimeException(
                    "Expect (tuple, number), input " + input.size() + " fields");
        }

        try {
            VectorConverter.checkVectorSchema(input.getField(0).schema);
            VectorConverter.checkVectorSchema(input.getField(1).schema);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        try {
            return input.getField(0).schema;
        } catch (FrontendException e) {
            throw new RuntimeException(e);
        }
    }
}
