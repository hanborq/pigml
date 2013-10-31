package org.pigml.udf.vector;

import org.apache.mahout.math.Vector;
import org.apache.mahout.math.function.Functions;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.pigml.utils.Env;
import org.pigml.utils.SchemaUtils;
import org.pigml.utils.VectorUtils;

import java.util.Iterator;

/**
 * Created with IntelliJ IDEA.
 * User: g
 * Date: 9/14/13
 * Time: 2:55 PM
 * To change this template use File | Settings | File Templates.
 */
public class VectorSum extends EvalFunc<Tuple> {
    private VectorUtils.Vector2Tuple tgen;
    private VectorUtils.Tuple2Vector vgen;

    public VectorSum() {
        this.tgen = VectorUtils.createTupleGenerator();
    }

    @Override
    public Tuple exec(Tuple input) throws ExecException, FrontendException {
        if (input == null || input.isNull(0)) {
            return null;
        }
        if (vgen == null) {
            vgen = VectorUtils.createVectorGenerator(Env.getProperty(this.getClass(), "vector.type"));
        }
        Vector vector = null;
        Iterator<Tuple> itor = ((DataBag) input.get(0)).iterator();
        while (vector == null && itor.hasNext()) {
            vector = vgen.apply(itor.next());
        }
        while (itor.hasNext()) {
            Vector v = vgen.apply(itor.next());
            if (v != null) {
                vector.assign(v, Functions.PLUS);
            }
        }
        return tgen.apply(vector);
    }

    @Override
    public Schema outputSchema(final Schema input) {
        //input should be bag of tuple, where tuple is the vector
        Schema.FieldSchema bag = SchemaUtils.claim(input, 0, DataType.BAG);
        Schema.FieldSchema tuple = SchemaUtils.claim(bag, 0, DataType.TUPLE);
        Env.setProperty(this.getClass(), "vector.type",
                VectorUtils.typeOfVector(tuple.schema));
        return tgen.getSchema();
    }
}
