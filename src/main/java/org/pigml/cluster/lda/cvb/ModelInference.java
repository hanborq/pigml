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
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.pigml.lang.FX;
import org.pigml.lang.Pair;
import org.pigml.utils.Env;
import org.pigml.utils.VectorUtils;

import java.io.IOException;

import static org.pigml.cluster.lda.cvb.ModelUtils.fCountTerms;
import static org.pigml.cluster.lda.cvb.ModelUtils.fLoadModel;
import static org.pigml.utils.Functors.*;

/**
 * Created with IntelliJ IDEA.
 * User: g
 * Date: 9/14/13
 * Time: 11:27 AM
 * To change this template use File | Settings | File Templates.
 */
public class ModelInference extends EvalFunc<Tuple> implements Constants {

    private static final Log LOG = LogFactory.getLog(ModelInference.class);

    private TopicModel readModel;
    private int numTopics;
    private int numTerms;
    private int maxIters;
    private VectorUtils.Tuple2Vector vgen;
    private VectorUtils.Vector2Tuple tgen;

    public ModelInference(final String modelloc) throws IOException {
        tgen = VectorUtils.createTupleGenerator();
        Env.inBackground(new Env.BackgroundProcedure() {
            @Override
            public void execute(Configuration conf) throws IOException {
                numTopics = Env.getJobDefine(conf, NUM_TOPICS, AS_INT);
                numTerms = FX.lazyResolve(
                        Env.getJobDefine(conf, NUM_TERMS, AS_INT),
                        fCountTerms(conf, Env.getJobDefine(conf, DICTIONARY, AS_STRING)),
                        0);
                Preconditions.checkArgument(numTopics > 0,
                        "Illegal topic num(%d)", numTopics);
                Preconditions.checkArgument(numTerms > 0,
                        "Illegal term num(%d)", numTerms);
                maxIters = Env.getJobDefine(conf, MAX_ITERATIONS_PER_DOC, AS_INT,
                        String.valueOf(DEFAULT_MAX_ITERATIONS_PER_DOC));
                float eta = Env.getJobDefine(conf, TERM_TOPIC_SMOOTHING, AS_FLOAT,
                        String.valueOf(DEFAULT_TERM_TOPIC_SMOOTHING));
                float alpha = Env.getJobDefine(conf, DOC_TOPIC_SMOOTHING, AS_FLOAT,
                        String.valueOf(DEFAULT_DOC_TOPIC_SMOOTHING));
                float modelWeight = Env.getJobDefine(conf, MODEL_WEIGHT, AS_FLOAT,
                        String.valueOf(DEFAULT_MODEL_WEIGHT));
                Pair<Matrix, Vector> pair = FX.any(fLoadModel(conf, modelloc));
                Preconditions.checkNotNull(pair, "No model applicable for inference");
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
        Vector docTopics = new DenseVector(numTopics).assign(1.0 / numTopics);
        Matrix docModel = new SparseRowMatrix(numTopics, /*document.size()*/numTerms);
        readModel.train(document, docTopics, docModel, maxIters);
        return tgen.apply(docTopics);
    }

    @Override
    public Schema outputSchema(final Schema input) {
        Env.setProperty(this.getClass(), "vector.type", VectorUtils.typeOfVector(input));
        return tgen.getSchema();
    }
}
