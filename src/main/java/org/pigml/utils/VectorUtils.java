package org.pigml.utils;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.pig.ResourceSchema;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.pigml.lang.FX;
import org.pigml.lang.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: g
 * Date: 9/14/13
 * Time: 11:41 AM
 * To change this template use File | Settings | File Templates.
 */
public class VectorUtils {
    /*
    ref: http://grepcode.com/file/repo1.maven.org/maven2/com.twitter.elephantbird/elephant-bird-mahout/3.0.1/com/twitter/elephantbird/pig/mahout/VectorWritableConverter.java

 pair = LOAD '$data' AS (key: int, val: (cardinality: int, entries: {entry: (index: int, value: double)}));
 STORE pair INTO '$output' USING $SEQFILE_STORAGE (
   '-c $INT_CONVERTER', '-c $VECTOR_CONVERTER -- -sequential'
 );
     */

    static public interface Tuple2Elements {
        public Iterable<Pair<Integer, Double>> apply(Tuple t) throws ExecException;
    }

    static public interface Tuple2Vector /*extends Function<Tuple, Vector>*/ {
        public Vector apply(Tuple t) throws ExecException;
    }

    static public interface Vector2Tuple extends Function<Vector, Tuple> {
        Schema getSchema();
    }

    static public interface VectorTupleFactory {
        Tuple create(int cardinality, Iterable<Pair<Integer, Double>> map);
    }

    static public VectorTupleFactory getVectorTupleFactory() {
        return new VectorTupleFactory() {
            private TupleFactory tfac = TupleFactory.getInstance();
            private BagFactory bfac = BagFactory.getInstance();

            public Tuple create(int cardinality, Iterable<Pair<Integer, Double>> features) {
                List<Tuple> tfeatures = new ArrayList<Tuple>();
                for (Pair<Integer, Double> p : features) {
                    tfeatures.add(tfac.newTupleNoCopy(Arrays.asList(p.getFirst(), p.getSecond())));
                }
                return tfac.newTupleNoCopy(Arrays.asList(cardinality, bfac.newDefaultBag(tfeatures)));
            }
        };
    }

    static public Vector2Tuple createTupleGenerator() {
        return new Vector2Tuple() {
            private final TupleFactory tfac = TupleFactory.getInstance();
            private final BagFactory bfac = BagFactory.getInstance();

            @Override
            public Tuple apply(Vector v) {
                if (v == null) {
                    return null;
                }
                List<Tuple> elmnts = new ArrayList<Tuple>(v.getNumNonZeroElements());
                for (Vector.Element x : v.nonZeroes()) {
                    elmnts.add(tfac.newTupleNoCopy(Arrays.asList(x.index(), x.get())));
                }
                return tfac.newTuple(Arrays.asList(v.size(), bfac.newDefaultBag(elmnts)));
            }

            @Override
            public Schema getSchema() {
                return SchemaUtils.schemaOf(
                        SchemaUtils.primitiveOf("cardinality", DataType.INTEGER),
                        SchemaUtils.bagOf("features",
                                SchemaUtils.primitiveOf("index", DataType.INTEGER),
                                SchemaUtils.primitiveOf("value", DataType.DOUBLE)
                        )
                );
/*
                Schema inner = new Schema(Arrays.asList(
                        new Schema.FieldSchema("index", DataType.INTEGER),
                        new Schema.FieldSchema("value", DataType.DOUBLE)));
                try {
                    return new Schema(Arrays.asList(
                            new Schema.FieldSchema("cardinality", DataType.INTEGER),
                            new Schema.FieldSchema("vector", inner, DataType.BAG)));
                } catch (FrontendException e) {
                    throw new RuntimeException(e);
                }
*/
            }
        };
    }

    //valid input:
    //sparse vector: (cardinality: int,entries: {(index: int,value: double)})
    //dense vector: (double,...)
    //dense vector(float): (float,...)
    static public String typeOfVector(Schema schema) {
        Schema.FieldSchema bag = SchemaUtils.tryClaim(schema, 1, DataType.BAG);
        if (bag != null) {
            Schema.FieldSchema tuple = SchemaUtils.claim(bag, 0, DataType.TUPLE);
            boolean isDouble = SchemaUtils.matches(tuple.schema, DataType.INTEGER, DataType.DOUBLE);
            boolean isFloat = SchemaUtils.matches(tuple.schema, DataType.INTEGER, DataType.FLOAT);
            Preconditions.checkArgument(isDouble || isFloat,
                    "Bag %s ('%s') is invalid for vector features", bag.alias, bag.schema);
            return "sparse";
        } else {
            /*System.out.println("Dumping schema");
            dump(schema, null);*/
            throw new RuntimeException(
                    String.format("TODO dense is not supported. Is '%s' a dense? ", schema));
        }
    }

    public static String typeOfVector(ResourceSchema schema) throws FrontendException {
        ResourceSchema.ResourceFieldSchema bag = SchemaUtils.tryClaim(schema, 1, DataType.BAG);
        if (bag != null) {
            ResourceSchema.ResourceFieldSchema tuple = SchemaUtils.claim(bag, 0, DataType.TUPLE);
            boolean isDouble = SchemaUtils.matches(tuple.getSchema(), DataType.INTEGER, DataType.DOUBLE);
            boolean isFloat = SchemaUtils.matches(tuple.getSchema(), DataType.INTEGER, DataType.FLOAT);
            Preconditions.checkArgument(isDouble || isFloat,
                    "Bag %s ('%s') is invalid for vector features", bag.getName(), bag.getSchema());
            return "sparse";
        } else {
            throw new RuntimeException(
                    String.format("TODO dense is not supported. Is '%s' a dense? ", schema));
        }
    }

    static public Tuple2Elements createElementsGenerator(String type) {
        Preconditions.checkArgument("sparse".equals(type), "TODO: %s is not supported for now", type);
        return new Tuple2Elements() {
            @Override
            public Iterable<Pair<Integer, Double>> apply(Tuple tuple) throws ExecException {
                return Iterables.transform((DataBag) tuple.get(1), new Function<Tuple, Pair<Integer, Double>>() {
                    @Override
                    public Pair<Integer, Double> apply(Tuple input) {
                        if (input != null) {
                            try {
                                return Pair.of((Integer)input.get(0), (Double)input.get(1));
                            } catch (ExecException e) {
                                throw new RuntimeException(e);
                            }
                        }
                        return null;
                    }
                });
            }
        };
    }

    static public Tuple2Vector createVectorGenerator(String type) throws FrontendException {
        return "sparse".equals(type) ?
                createSparseVectorGenerator() :
                createDenseVectorGenerator();
    }

    static public Tuple2Vector createDenseVectorGenerator() {
        return new Tuple2Vector() {
            @Override
            public Vector apply(Tuple input) throws ExecException {
                if (input == null) {
                    return null;
                }
                double[] elmnts = new double[input.size()];
                for (int i=0; i<elmnts.length; i++) {
                    elmnts[i] = (Double)input.get(i);
                }
                return new DenseVector(elmnts, true);
            }
        };
    }

    static public Tuple2Vector createSparseVectorGenerator() {
        return new Tuple2Vector() {
            @Override
            public Vector apply(Tuple tuple) throws ExecException {
                if (tuple == null) {
                    return null;
                }
                int cardinality = (Integer)tuple.get(0);
                DataBag bag = (DataBag) tuple.get(1);
                Vector v = new RandomAccessSparseVector(cardinality, (int) bag.size());
                for (Tuple t : bag) {
                    int index = (Integer)t.get(0);
                    double value = (Double)t.get(1);
                    v.setQuick(index, value);
                }
                return v;
            }
        };
    }


    static public void dump(Tuple input, String prefix) throws ExecException {
        prefix = FX.any(prefix, "__");
        System.out.printf("%s dump input %d\n", prefix, input.size());
        for (int i=0; i<input.size(); i++) {
            Object obj = input.get(i);
            System.out.printf("%s dump(%d) type=%s val=%s\n", prefix, i, obj.getClass().getSimpleName(), obj);
            if (obj instanceof Tuple) {
                dump((Tuple)obj, prefix+"__");
            }
        }
    }
    static void dump(Schema schema, String ident) {
        ident = FX.any(ident, "  ");
        for (Schema.FieldSchema f : schema.getFields()) {
            System.out.printf("%s field %s = %s\n", ident, f.alias, DataType.findTypeName(f.type));
            if (f.schema != null) {
                dump(f.schema, ident+"  ");
            }
        }
    }
}
