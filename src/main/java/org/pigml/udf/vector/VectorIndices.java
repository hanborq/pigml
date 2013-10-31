package org.pigml.udf.vector;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.pigml.lang.Pair;
import org.pigml.utils.Env;
import org.pigml.utils.VectorUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: g
 * Date: 9/14/13
 * Time: 2:55 PM
 * To change this template use File | Settings | File Templates.
 */
public class VectorIndices extends EvalFunc<DataBag> {
    private VectorUtils.Tuple2Elements elmntsGen;
    private BagFactory bfac = BagFactory.getInstance();
    private TupleFactory tfac = TupleFactory.getInstance();

    public VectorIndices() {
    }

    @Override
    public DataBag exec(Tuple input) throws ExecException, FrontendException {
        if (input == null || input.isNull(0)) {
            return null;
        }
        if (elmntsGen == null) {
            elmntsGen = VectorUtils.createElementsGenerator(Env.getProperty(this.getClass(), "vector.type"));
        }
        Iterable<Pair<Integer,Double>> elements = elmntsGen.apply((Tuple) input.get(0));
        List<Tuple> results = new ArrayList<Tuple>();
        for (Pair<Integer, Double> pair : elements) {
            results.add(tfac.newTuple(pair.getFirst()));
        }
        return bfac.newDefaultBag(results);
    }

    //sample input schema:
    //  (cardinality: int,vector: {(index: int,value: double)})
    @Override
    public Schema outputSchema(final Schema input) {
        try {
            Env.setProperty(this.getClass(), "vector.type",
                    VectorUtils.typeOfVector(input.getField(0).schema));
            return new Schema(new Schema.FieldSchema(
                    null, new Schema(new Schema.FieldSchema(null, DataType.INTEGER)), DataType.BAG));
        } catch (FrontendException e) {
            throw new RuntimeException(e);
        }
    }
}
