package org.pigml.cluster.lda.cvb;

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.SparseRowMatrix;
import org.apache.mahout.math.Vector;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.pigml.lang.FX;
import org.pigml.lang.Pair;
import org.pigml.utils.Env;
import org.pigml.utils.VectorUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import static org.pigml.cluster.lda.cvb.ModelUtils.*;
import static org.pigml.utils.Functors.*;

/**
 * Created with IntelliJ IDEA.
 * User: g
 * Date: 9/14/13
 * Time: 11:27 AM
 * To change this template use File | Settings | File Templates.
 */
public class ModelPerplexity extends EvalFunc<Tuple> implements Constants {

    private static final Log LOG = LogFactory.getLog(ModelPerplexity.class);

    private TopicModel readModel;
    private int numTopics;
    private int maxIters;
    private int numTerms;
    private TupleFactory tfac;
    private VectorUtils.Tuple2Vector vgen;

    public ModelPerplexity(String modelloc) throws IOException {
        this(modelloc, "-1");
    }

    public ModelPerplexity(final String modelloc, final String iter) throws IOException {
        Env.inBackground(new Env.BackgroundProcedure() {
            @Override
            public void execute(Configuration conf) throws IOException {
                tfac = TupleFactory.getInstance();
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
                Pair<Matrix, Vector> pair = FX.any(fLoadModel(conf, modelloc, Integer.parseInt(iter)), fRandomModel(numTopics, numTerms));
                readModel = new TopicModel(pair.getFirst(), pair.getSecond(),
                        eta, alpha, null, modelWeight);
                LOG.info(String.format("Ready to go with numTopics %d numTerms %d eta%f alpha %f",
                        numTopics, numTerms, eta, alpha));
            }
        });
    }

    @Override
    public Tuple exec(Tuple input) throws IOException {
        if (vgen == null) {
            vgen = VectorUtils.createVectorGenerator(Env.getProperty(this.getClass(), "vector.type"));
        }
        Vector document = vgen.apply(input);
        double key = document.norm(1);

        Vector topicVector = new DenseVector(new double[numTopics]).assign(1.0 / numTopics);
        Matrix docTopicModel = new SparseRowMatrix(numTopics, numTerms, true);
        readModel.train(document, topicVector, docTopicModel, maxIters);
        double value = readModel.perplexity(document, topicVector);
        return tfac.newTupleNoCopy(Arrays.asList(key, value));
    }

    @Override
    public Schema outputSchema(final Schema input) {
        Env.setProperty(this.getClass(), "vector.type", VectorUtils.typeOfVector(input));
        return new Schema(Arrays.asList(
                new Schema.FieldSchema("key", DataType.DOUBLE),
                new Schema.FieldSchema("value", DataType.DOUBLE)));
    }
}
