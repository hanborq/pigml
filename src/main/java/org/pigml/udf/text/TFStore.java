package org.pigml.udf.text;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.mahout.math.map.OpenObjectIntHashMap;
import org.apache.pig.ResourceSchema;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.pigml.lang.FX;
import org.pigml.lang.Pair;
import org.pigml.lang.Procedure;
import org.pigml.storage.IterableProxyStore;
import org.pigml.storage.vector.VectorStore;
import org.pigml.utils.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: g
 * Date: 13-9-26
 * Time: 下午3:28
 * To change this template use File | Settings | File Templates.
 */
public class TFStore extends IterableProxyStore {

    private static final Log LOG = LogFactory.getLog(DictStore.class);

    private OpenObjectIntHashMap<String> dictionary;
    private Integer numTerms;
    private TupleFactory tfac;
    private VectorUtils.VectorTupleFactory vtfac;
    private HashMap<Integer, Integer> workmap;

    public TFStore() throws ParseException, IOException, ClassNotFoundException {
        this("string");
    }

    public TFStore(String keyType) throws IOException, ParseException, ClassNotFoundException {
        super(new VectorStore(keyType));
        Env.inBackground(new Env.BackgroundProcedure() {
            @Override
            public void execute(Configuration conf) throws IOException {
                workmap = new HashMap<Integer, Integer>();
                tfac = TupleFactory.getInstance();
                vtfac = VectorUtils.getVectorTupleFactory();
            }
        });
    }

    @Override
    protected void forIteration(Configuration conf, Path partfile, String iterover) throws IOException {
        dictionary = new OpenObjectIntHashMap<String>();
        IOUtils.forEachKeyValue(conf, partfile,
                new Procedure<Pair<Writable, IntWritable>>() {
                    @Override
                    public void execute(Pair<Writable, IntWritable> pair) {
                        dictionary.put(pair.getFirst().toString(), pair.getSecond().get());
                    }
                });
        IOUtils.forEachLines(conf, new Path(iterover, DICT_CARDINAL_FILE),
                new Procedure<String>() {
                    @Override
                    public void execute(String line) throws ProcedureEndException {
                        Preconditions.checkState(numTerms == null);
                        numTerms = Integer.parseInt(line);
                    }
                });
    }

    @Override
    public void putNext(Tuple tuple) throws IOException {
        Object name = tuple.get(0);
        DataBag bag = (DataBag) tuple.get(1);
        workmap.clear();
        for (Tuple t : bag) {
            String term = (String) t.get(0);
            if (dictionary.containsKey(term)) {
                int termId = dictionary.get(term);
                workmap.put(termId, FX.any(workmap.get(termId), 0) + 1);
            }
        }
        Iterable<Pair<Integer, Double>> features = Iterables.transform(
                workmap.entrySet(),
                new Function<Map.Entry<Integer, Integer>, Pair<Integer, Double>>() {
                    @Override
                    public Pair<Integer, Double> apply(Map.Entry<Integer, Integer> input) {
                        return new Pair<Integer, Double>(
                                input.getKey(), (double) input.getValue());
                    }
                });
        super.putNext(tfac.newTupleNoCopy(Arrays.asList(
                name, vtfac.create(numTerms, features))));
    }

    @Override
    public void checkSchema(ResourceSchema s) throws IOException {
        SchemaUtils.claim(s, 0, DataType.CHARARRAY);
        ResourceSchema.ResourceFieldSchema bag = SchemaUtils.claim(s, 1, DataType.BAG);
        ResourceSchema.ResourceFieldSchema tuple = SchemaUtils.claim(bag, 0, DataType.TUPLE);
        SchemaUtils.claim(tuple, 0, DataType.CHARARRAY);
    }
}
