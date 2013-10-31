package org.pigml.classify.naivebayes;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.math.DenseVector;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.UDFContext;
import org.pigml.utils.VectorConverter;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.pigml.utils.VectorConverter.VectorType.CARDINALITY_SPARSE;
import static org.pigml.utils.VectorConverter.VectorType.DENSE;

/**
 * Calculate a score of a corpus for a specified label.
 */
public class LabelScores extends EvalFunc<Tuple> {

    private String modelDir;
    private NaiveBayesModel model;
    private StandardNaiveBayesClassifier classifier;


    public LabelScores(String modelDir) {
        this.modelDir = modelDir;
    }

    @Override
    public Tuple exec(Tuple input) throws IOException {
        if (this.model == null) {
            initializeModel();
        }

        Tuple vector = (Tuple) input.get(0);
        DataBag entries = (DataBag) vector.get(1);

        int labelNum = model.numLabels();
        double[] scores = new double[labelNum];
        for (int label = 0; label < labelNum; ++label) {
            double score = 0;
            for (Tuple entry : entries) {
                int feature = (Integer) entry.get(0);
                double value = (Double) entry.get(1);
                score += value * classifier.getScoreForLabelFeature(label, feature);
            }
            scores[label] = score;
        }

        // return a dense vector
        DenseVector dv = new DenseVector(scores);
        return VectorConverter.convertToTuple(dv, DENSE);
    }

    private void initializeModel() throws IOException {
        Configuration conf = UDFContext.getUDFContext().getJobConf();
        Path modelDir = new Path("file://" + new File("model").getAbsoluteFile());
        model = NaiveBayesModel.materialize(modelDir, conf);
        classifier = new StandardNaiveBayesClassifier(model);
    }

    @Override
    public Schema outputSchema(Schema input) {
        Preconditions.checkArgument(input.size() == 1,
                "Expect (tuple), input " + input.size() + " fields.");

        try {
            // the first field is a vector to calculate the score
            Schema vectorSchema = input.getField(0).schema;
            VectorConverter.checkVectorSchema(vectorSchema, CARDINALITY_SPARSE);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        try {
            return VectorConverter.newVectorSchema(DENSE);
        } catch (FrontendException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<String> getCacheFiles() {
        return ImmutableList.of(this.modelDir + "#model");
    }
}
