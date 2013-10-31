package org.pigml.udf.vector;

import org.apache.hadoop.conf.Configuration;
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

import java.io.IOException;
import java.util.Iterator;

/**
 * Created with IntelliJ IDEA.
 * User: g
 * Date: 9/14/13
 * Time: 2:55 PM
 * To change this template use File | Settings | File Templates.
 */
public class VectorMerge extends EvalFunc<Tuple> {

    public static final float NO_NORMALIZING = -1.0f;

    private VectorUtils.Vector2Tuple tgen;
    private VectorUtils.Tuple2Vector vgen;
    private float normPower;
    private boolean logNormalize;

    public VectorMerge() throws IOException {
        this(String.valueOf(NO_NORMALIZING), "false");
    }

    public VectorMerge(String normPower, String logNormalize) throws IOException {
        this.tgen = VectorUtils.createTupleGenerator();
        this.normPower = Float.parseFloat(normPower);
        this.logNormalize = Boolean.parseBoolean(logNormalize);
        Env.inBackground(new Env.BackgroundProcedure() {
            @Override
            public void execute(Configuration conf) throws IOException {
                vgen = VectorUtils.createVectorGenerator(
                        Env.getProperty(VectorMerge.class, "vector.type"));
            }
        });
    }

    @Override
    public Tuple exec(Tuple input) throws ExecException, FrontendException {
        if (input == null || input.isNull(0)) {
            return null;
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
        if (normPower != NO_NORMALIZING) {
            if (logNormalize) {
                vector = vector.logNormalize(normPower);
            } else {
                vector = vector.normalize(normPower);
            }
        }
        return tgen.apply(vector);
    }

    //sample input schema:
    //  {(cardinality: int,entries: {(index: int,value: double)})}
    @Override
    public Schema outputSchema(final Schema input) {
        Schema.FieldSchema bag = SchemaUtils.claim(input, 0, DataType.BAG);
        Schema.FieldSchema tuple = SchemaUtils.claim(bag, 0, DataType.TUPLE);
        Env.setProperty(VectorMerge.class, "vector.type",
                VectorUtils.typeOfVector(tuple.schema));
        return tgen.getSchema();
    }
}
