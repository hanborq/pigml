package org.pigml.udf.text;

import com.google.common.base.Preconditions;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.map.OpenIntLongHashMap;
import org.apache.mahout.vectorizer.TFIDF;
import org.apache.pig.ResourceSchema;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.pigml.lang.Pair;
import org.pigml.lang.Procedure;
import org.pigml.storage.IterableProxyStore;
import org.pigml.storage.vector.VectorStore;
import org.pigml.utils.*;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created with IntelliJ IDEA.
 * User: g
 * Date: 13-9-26
 * Time: 下午3:28
 * To change this template use File | Settings | File Templates.
 */
public class TFIDFStore extends IterableProxyStore {

    private static final Log LOG = LogFactory.getLog(TFIDFStore.class);

    private OpenIntLongHashMap dictionary;
    private int totalDocuments;
    private int totalFeatures;
    private VectorUtils.Tuple2Elements elmntsGen;
    private VectorUtils.Vector2Tuple tgen;
    private TupleFactory tfac;
    private TFIDF tfidf;
    private long maxDF;
    private long minDF;

    public TFIDFStore() throws ParseException, IOException, ClassNotFoundException {
        this("string");
    }

    public TFIDFStore(String keyType) throws IOException, ParseException, ClassNotFoundException {
        super(new VectorStore(keyType));
        Env.inBackground(new Env.BackgroundProcedure() {
            @Override
            public void execute(Configuration conf) throws IOException {
                totalDocuments = Integer.parseInt(
                        IOUtils.readLine(conf,
                                PathUtils.getParts(conf, Env.getJobDefine(conf, "tfidftotal"), 0)));
                totalFeatures = Integer.parseInt(IOUtils.readLine(conf,
                        new Path(Env.getJobDefine(conf, "dictdir"), DICT_CARDINAL_FILE)));
                elmntsGen = VectorUtils.createElementsGenerator(
                        Env.getProperty(TFIDFStore.class, "vector.type"));
                minDF = Env.getJobDefine(conf, "mindf", Functors.AS_LONG, "1");
                float P = Env.getJobDefine(conf, "maxdfpercent", Functors.AS_FLOAT, "0.0");
                maxDF = P > 0 ? (long) (totalDocuments * (P / 100.0)) : 0;
                tfidf = new TFIDF();
                tfac = TupleFactory.getInstance();
                tgen = VectorUtils.createTupleGenerator();
                Preconditions.checkState(totalDocuments > 0 && totalFeatures > 0);
                LOG.info(String.format("Setup total documents %d, total features %d, minDF %d, maxDF %d",
                        totalDocuments, totalFeatures, minDF, maxDF));
            }
        });
    }

    @Override
    protected void forIteration(Configuration conf, Path partfile, String iterover) throws IOException {
        dictionary = new OpenIntLongHashMap();
        IOUtils.forEachKeyValue(conf, partfile,
                new Procedure<Pair<IntWritable, LongWritable>>() {
                    @Override
                    public void execute(Pair<IntWritable, LongWritable> pair) {
                        dictionary.put(pair.getFirst().get(), pair.getSecond().get());
                    }
                });
    }

    @Override
    public void putNext(Tuple tuple) throws IOException {
        if (tuple == null) {
            return;
        }
        Object name = tuple.get(0);
        Vector v = new RandomAccessSparseVector(totalFeatures);
        Iterable<Pair<Integer,Double>> elmnts = elmntsGen.apply((Tuple) tuple.get(1));
        for (Pair<Integer, Double> pair : elmnts) {
            if (dictionary.containsKey(pair.getFirst())) {
                long df = dictionary.get(pair.getFirst());
                if (df >= minDF && (maxDF == 0 || df <= maxDF)) {
                    v.setQuick(pair.getFirst(),
                            tfidf.calculate((int)(double)pair.getSecond(), (int) df, 0, totalDocuments));
                }
            }
        }
        super.putNext(tfac.newTupleNoCopy(Arrays.asList(name, tgen.apply(v))));
    }

    @Override
    public void checkSchema(ResourceSchema s) throws IOException {
        ResourceSchema.ResourceFieldSchema tuple = SchemaUtils.claim(s, 1, DataType.TUPLE);
        Env.setProperty(TFIDFStore.class, "vector.type",
                VectorUtils.typeOfVector(tuple.getSchema()));
    }
}
