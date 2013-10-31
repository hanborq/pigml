package org.pigml.cluster.lda.cvb;

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.mahout.math.*;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.pigml.lang.FX;
import org.pigml.lang.Pair;
import org.pigml.utils.Env;
import org.pigml.utils.SchemaUtils;
import org.pigml.utils.VectorUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.pigml.cluster.lda.cvb.ModelUtils.*;
import static org.pigml.utils.Functors.*;

/**
 * Created with IntelliJ IDEA.
 * User: g
 * Date: 9/14/13
 * Time: 10:49 AM
 * To change this template use File | Settings | File Templates.
 */
public class ModelTrain extends EvalFunc<DataBag> implements Constants {

    private static final Log LOG = LogFactory.getLog(ModelUtils.class);

    private int numTopics;
    private int numTerms;
    private int maxIters;
    private TopicModel readModel;
    private TopicModel writeModel;

    private final BagFactory bfac = DefaultBagFactory.getInstance();
    private final TupleFactory tfac = TupleFactory.getInstance();
    private VectorUtils.Vector2Tuple tgen;
    private VectorUtils.Tuple2Vector vgen;

    public ModelTrain(final String modelloc) throws IOException {
        tgen = VectorUtils.createTupleGenerator();
        Env.inBackground(new Env.BackgroundProcedure() {
            @Override
            public void execute(Configuration conf) throws IOException {
                numTopics = Env.getJobDefine(conf, NUM_TOPICS, AS_INT);
                numTerms = FX.lazyResolve(
                        Env.getJobDefine(conf, NUM_TERMS, AS_INT),
                        fCountTerms(conf, Env.getJobDefine(conf, DICTIONARY, AS_STRING)),
                        0);
                Preconditions.checkArgument(numTopics > 0 && numTerms > 0,
                        "Illegal topic/term num(%d/%d)", numTopics, numTerms);
                maxIters = Env.getJobDefine(conf, MAX_ITERATIONS_PER_DOC, AS_INT,
                        String.valueOf(DEFAULT_MAX_ITERATIONS_PER_DOC));
                float eta = Env.getJobDefine(conf, TERM_TOPIC_SMOOTHING, AS_FLOAT,
                        String.valueOf(DEFAULT_TERM_TOPIC_SMOOTHING));
                float alpha = Env.getJobDefine(conf, DOC_TOPIC_SMOOTHING, AS_FLOAT,
                        String.valueOf(DEFAULT_DOC_TOPIC_SMOOTHING));
                float modelWeight = Env.getJobDefine(conf, MODEL_WEIGHT, AS_FLOAT,
                        String.valueOf(DEFAULT_MODEL_WEIGHT));
                Pair<Matrix, Vector> pair = FX.any(
                        fLoadModel(conf, modelloc), fRandomModel(numTopics, numTerms));
                readModel = new TopicModel(pair.getFirst(), pair.getSecond(),
                        eta, alpha, null, modelWeight);
                pair = FX.any(fRandomModel(numTopics, numTerms));
                writeModel = new TopicModel(pair.getFirst(), pair.getSecond(),
                        eta, alpha, null, modelWeight);
                vgen = VectorUtils.createVectorGenerator(Env.getProperty(ModelTrain.class, "vector.type"));
                LOG.info(String.format("Ready to go with numTopics %d numTerms %d eta%f alpha %f",
                        numTopics, numTerms, eta, alpha));
            }
        });
    }

    @Override
    public DataBag exec(Tuple input) throws IOException {
        if (input == null || input.size() < 1) {
            return null;
        }
        DataBag bag = (DataBag) input.get(0);
        for (Tuple t : bag) {
            Vector document = vgen.apply(t);
            Vector topicVector = new DenseVector(numTopics).assign(1.0 / numTopics);
            Matrix docTopicModel = new SparseRowMatrix(numTopics, numTerms, true);
            readModel.train(document, topicVector, docTopicModel, maxIters);
            writeModel.update(docTopicModel);
        }
        List<Tuple> tuples = new ArrayList<Tuple>();
        for (MatrixSlice topic : writeModel) {
            tuples.add(tfac.newTupleNoCopy(
                    Arrays.asList(topic.index(), tgen.apply(topic.vector()))));
        }
        return bfac.newDefaultBag(tuples);
    }

    //input schema example
    //  {shuffed: {(cardinality: int,entries: {t: (index: int,value: double)})}}
    @Override
    public Schema outputSchema(final Schema input) {
        Schema.FieldSchema bag = SchemaUtils.claim(input, 0, DataType.BAG);
        Schema.FieldSchema tuple = SchemaUtils.claim(bag, 0, DataType.TUPLE);
        Env.setProperty(ModelTrain.class, "vector.type", VectorUtils.typeOfVector(tuple.schema));
        return new Schema(Arrays.asList(
                new Schema.FieldSchema("index", DataType.INTEGER),
                new Schema.FieldSchema("value", tgen.getSchema())));
    }
}
