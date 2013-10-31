package org.pigml.udf.vector;

import com.google.common.collect.Ordering;
import org.apache.hadoop.conf.Configuration;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.pigml.lang.Pair;
import org.pigml.utils.Env;
import org.pigml.utils.SchemaUtils;
import org.pigml.utils.VectorUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: g
 * Date: 9/14/13
 * Time: 2:55 PM
 * To change this template use File | Settings | File Templates.
 */

//get top N value in a vector
public class TopValue extends EvalFunc<DataBag> {
    private final int N;
    private VectorUtils.Tuple2Elements elgen;
    private Ordering<Pair<Integer,Double>> ordering;
    private TupleFactory tfac;
    private BagFactory bfac;

    public TopValue(String N) throws IOException {
        this.N = Integer.parseInt(N);
        Env.inBackground(new Env.BackgroundProcedure() {
            @Override
            public void execute(Configuration conf) throws IOException {
                tfac = TupleFactory.getInstance();
                bfac = BagFactory.getInstance();
                elgen = VectorUtils.createElementsGenerator(
                        Env.getProperty(TopValue.class, "vector.type"));
                ordering = Ordering.from(new Comparator<Pair<Integer,Double>>() {
                    @Override
                    public int compare(Pair<Integer,Double> o1, Pair<Integer,Double> o2) {
                        final double d1 = o1.getSecond();
                        final double d2 = o2.getSecond();
                        return d1 < d2 ? -1 : (d1 > d2 ? 1 : 0);
                    }
                });
            }
        });
    }

    @Override
    public DataBag exec(Tuple input) throws ExecException, FrontendException {
        if (input == null) {
            return null;
        }
        Iterable<Pair<Integer,Double>> vector = elgen.apply(input);
        List<Tuple> result = new ArrayList<Tuple>(N);
        for (Pair<Integer, Double> el : ordering.greatestOf(vector, N)) {
            result.add(tfac.newTuple(el.getSecond()));
        }
        return bfac.newDefaultBag(result);
    }

    @Override
    public Schema outputSchema(final Schema input) {
        Env.setProperty(TopValue.class, "vector.type", VectorUtils.typeOfVector(input));
        return SchemaUtils.schemaOf(SchemaUtils.bagOf(
                SchemaUtils.primitiveOf(DataType.DOUBLE)));
    }
}
