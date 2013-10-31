package org.pigml.udf.vector;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.mahout.math.Vector;
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
import java.util.Arrays;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: g
 * Date: 9/14/13
 * Time: 2:55 PM
 * To change this template use File | Settings | File Templates.
 */
public class VectorSplit extends EvalFunc<DataBag> {

    private VectorUtils.Vector2Tuple tgen;
    private VectorUtils.Tuple2Vector vgen;
    private VectorUtils.VectorTupleFactory vtfac;
    private BagFactory bfac;
    private TupleFactory tfac;
    private int splitBy;

    public VectorSplit(String splitBy) throws IOException {
        this.tgen = VectorUtils.createTupleGenerator();
        this.splitBy = Integer.parseInt(splitBy);
        Preconditions.checkArgument(this.splitBy > 0, "Illegal splitBy %s", splitBy);
        Env.inBackground(new Env.BackgroundProcedure() {
            @Override
            public void execute(Configuration conf) throws IOException {
                vgen = VectorUtils.createVectorGenerator(
                        Env.getProperty(VectorSplit.class, "vector.type"));
                bfac = BagFactory.getInstance();
                tfac = TupleFactory.getInstance();
                vtfac = VectorUtils.getVectorTupleFactory();
            }
        });
    }

    @Override
    public DataBag exec(Tuple input) throws ExecException, FrontendException {
        if (input == null || input.isNull(0)) {
            return null;
        }
        Vector vector = vgen.apply(input);
        List<Pair<Integer, Double>>[] childs = new ArrayList[splitBy];
        for (int i=0; i<childs.length; i++) {
            childs[i] = new ArrayList<Pair<Integer, Double>>();
        }
        for (Vector.Element elm : vector.nonZeroes()) {
            childs[elm.index() % splitBy].add(Pair.of(elm.index(), elm.get()));
        }
        List<Tuple> results = new ArrayList<Tuple>(splitBy);
        for (int i=0; i<childs.length; i++) {
            results.add(tfac.newTupleNoCopy(Arrays.asList(
                    i, vtfac.create(vector.size(), childs[i]))));
        }
        return bfac.newDefaultBag(results);
    }

    @Override
    public Schema outputSchema(final Schema input) {
        Env.setProperty(VectorSplit.class, "vector.type",
                VectorUtils.typeOfVector(input));
        return SchemaUtils.schemaOf(
                SchemaUtils.bagOf(
                        SchemaUtils.primitiveOf(DataType.INTEGER),
                        SchemaUtils.fieldOf(null, DataType.TUPLE, tgen.getSchema())));
    }
}
