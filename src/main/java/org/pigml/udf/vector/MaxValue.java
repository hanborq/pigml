package org.pigml.udf.vector;

import org.apache.mahout.math.Vector;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.pigml.utils.Env;
import org.pigml.utils.VectorUtils;

/**
 * Created with IntelliJ IDEA.
 * User: g
 * Date: 9/14/13
 * Time: 2:55 PM
 * To change this template use File | Settings | File Templates.
 */
public class MaxValue extends EvalFunc<Double> {
    private VectorUtils.Tuple2Vector vgen;

    @Override
    public Double exec(Tuple input) throws ExecException, FrontendException {
        if (input == null) {
            return null;
        }
        if (vgen == null) {
            vgen = VectorUtils.createVectorGenerator(Env.getProperty(this.getClass(), "vector.type"));
        }
        Vector vector = vgen.apply(input);
        return vector.maxValue();
    }

    @Override
    public Schema outputSchema(final Schema input) {
        Env.setProperty(this.getClass(), "vector.type", VectorUtils.typeOfVector(input));
        return new Schema(new Schema.FieldSchema(null, DataType.DOUBLE));
    }
}
