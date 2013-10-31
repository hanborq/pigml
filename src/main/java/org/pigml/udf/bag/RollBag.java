package org.pigml.udf.bag;

import com.google.common.base.Preconditions;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.pigml.lang.FX;
import org.pigml.utils.Env;
import org.pigml.utils.SchemaUtils;
import org.pigml.utils.VectorUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: g
 * Date: 13-10-22
 * Time: 下午3:49
 * To change this template use File | Settings | File Templates.
 */
//generate {(t1, t2, ...), (t2, t3, ...), ... } based on {t1, t2, ...}
public class RollBag extends EvalFunc<DataBag> {

    private final int stepSize;
    private final int bagSize;
    private final BagFactory bfac = BagFactory.getInstance();
    private final TupleFactory tfac = TupleFactory.getInstance();

    public RollBag(String argBagSize) {
        this(argBagSize, "1");
    }
    public RollBag(String argBagSize, String argStepSize) {
        this.stepSize = Integer.parseInt(argStepSize);
        this.bagSize = Integer.parseInt(argBagSize);
        Preconditions.checkArgument(stepSize > 0 && bagSize > 0);
    }

    @Override
    public DataBag exec(Tuple input) throws IOException {
        Iterator<Tuple> ibag = ((DataBag) input.get(0)).iterator();
        List<Tuple> cached = new ArrayList<Tuple>(bagSize);
        List<Tuple> result = new ArrayList<Tuple>();
        while (ibag.hasNext()) {
            cached.add(ibag.next());
            if (cached.size() == bagSize) {
                result.add(tfac.newTuple(cached));
                cached.remove(0);
                if (stepSize > 1) {
                    int skipped = 1;
                    while (cached.size() > 0 && skipped++ < stepSize) {
                        cached.remove(0);
                    }
                    while (ibag.hasNext() && skipped++ < stepSize) {
                        ibag.next();
                    }
                }
            }
        }
        return bfac.newDefaultBag(result);
    }

    @Override
    public Schema outputSchema(final Schema input) {
        Schema.FieldSchema bag = SchemaUtils.claim(input, 0, DataType.BAG);
        Schema.FieldSchema tuple = SchemaUtils.claim(bag, 0, DataType.TUPLE);
        return SchemaUtils.schemaOf(SchemaUtils.bagOf(
                SchemaUtils.tupleOf(FX.repeat(tuple, bagSize))));
    }
}
