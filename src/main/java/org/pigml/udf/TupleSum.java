package org.pigml.udf;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.pigml.lang.Factory;
import org.pigml.utils.Env;
import org.pigml.utils.SchemaUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: g
 * Date: 9/14/13
 * Time: 11:57 AM
 * To change this template use File | Settings | File Templates.
 */
public class TupleSum extends EvalFunc<Tuple> {

    private List<TheType> types;
    private final TupleFactory tfac = TupleFactory.getInstance();

    @Override
    public Tuple exec(Tuple input) throws IOException {
        if (input == null) {
            return null;
        }
        if (types == null) {
            Iterable<String> itor = Splitter.on(",").split(
                    Env.getProperty(this.getClass(), "tuplesum.types"));
            types = new ArrayList<TheType>();
            for (String it : itor) {
                Factory<TheType> factory = Preconditions.checkNotNull(
                        factories.get(Byte.valueOf(it)), "illegal field type %s", it);
                types.add(factory.create());
            }
        }
        DataBag bag = (DataBag) input.get(0);
        for (Tuple tuple : bag) {
            if (tuple != null && tuple.size() >= types.size()) {
                for (int i=0; i<types.size(); i++) {
                    Object f = tuple.get(i);
                    if (f != null) {
                        types.get(i).sumup(f);
                    }
                }
            }
        }
        List<Object> results = new ArrayList<Object>(types.size());
        for (int i=0; i<types.size(); i++) {
            results.add(types.get(i).finish());
        }
        return tfac.newTupleNoCopy(results);
    }

    @Override
    public Schema outputSchema(Schema input) {
        Schema.FieldSchema bag = SchemaUtils.claim(input, 0, DataType.BAG);
        Schema.FieldSchema tuple = SchemaUtils.claim(bag, 0, DataType.TUPLE);
        List<Schema.FieldSchema> fields = tuple.schema.getFields();
        List<String> types = new ArrayList<String>();
        for (Schema.FieldSchema fs : fields) {
            Preconditions.checkArgument(factories.containsKey(fs.type),
                    "TODO: unsupported field type %s for field %s",
                    DataType.findTypeName(fs.type), fs.alias);
            types.add(String.valueOf(fs.type));
        }
        Env.setProperty(this.getClass(), "tuplesum.types", Joiner.on(",").join(types));
        return new Schema(fields);
    }

    static private interface TheType<T> {
        void sumup(Object value);
        T finish(); //finish and reset
    }

    static private final Map<Byte, Factory> factories;

    static {
        factories = new HashMap<Byte, Factory>();
        factories.put(DataType.DOUBLE, new Factory<TheType<Double>>() {
            @Override
            public TheType<Double> create() {
                return new TheType<Double>() {
                    private double value;

                    @Override
                    public void sumup(Object a) {
                        value += (Double) a;
                    }

                    @Override
                    public Double finish() {
                        double ret = value;
                        value = 0;
                        return ret;
                    }
                };
            }
        });
        factories.put(DataType.FLOAT, new Factory<TheType<Float>>() {
            @Override
            public TheType<Float> create() {
                return new TheType<Float>() {
                    private float value;

                    @Override
                    public void sumup(Object a) {
                        value += (Float) a;
                    }

                    @Override
                    public Float finish() {
                        float ret = value;
                        value = 0;
                        return ret;
                    }
                };
            }
        });
        factories.put(DataType.INTEGER, new Factory<TheType<Integer>>() {
            @Override
            public TheType<Integer> create() {
                return new TheType<Integer>() {
                    private int value;

                    @Override
                    public void sumup(Object a) {
                        value += (Integer) a;
                    }

                    @Override
                    public Integer finish() {
                        int ret = value;
                        value = 0;
                        return ret;
                    }
                };
            }
        });
    }
}
