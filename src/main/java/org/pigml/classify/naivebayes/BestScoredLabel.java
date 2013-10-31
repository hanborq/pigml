package org.pigml.classify.naivebayes;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.UDFContext;
import org.pigml.utils.VectorConverter;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.pigml.utils.VectorConverter.VectorType.*;

/**
 * Accept a vector of scores in which the position of each corresponds to a
 * label, output a label whose score is the best.
 */
public class BestScoredLabel extends EvalFunc<String> {

    private String labelIndexFile;

    private LabelIndex labelIndex;


    /**
     * The constructor accept a label index file which is used to map index (
     * of vector) to label name.
     * @param labelIndexFile the label index file path on HDFS.
     */
    public BestScoredLabel(String labelIndexFile) {
        this.labelIndexFile = labelIndexFile;
    }

    @Override
    public String exec(Tuple input) throws IOException {
        if (labelIndex == null) {
            initializeLabelIndex();
        }

        Tuple tuple = (Tuple) input.get(0);
        int bestIdx = Integer.MIN_VALUE;
        double bestScore = Long.MIN_VALUE;
        for (int i = 0; i < tuple.size(); ++i) {
            double score = (Double) tuple.get(i);
            if ( score > bestScore) {
                bestScore = score;
                bestIdx = i;
            }
        }

        return labelIndex.getLabel(bestIdx);
    }

    private void initializeLabelIndex() {
        Configuration conf = UDFContext.getUDFContext().getJobConf();
        Path liDir = new Path("file://" + new File("labelindex").getAbsoluteFile());
        labelIndex = LabelIndex.materialize(liDir, conf);
    }

    @Override
    public Schema outputSchema(Schema input) {
        Preconditions.checkArgument(input.size() == 1,
                "Expect ((double, double, ...)), input " + input.size() + " fields");
        try {
            Schema vectorSchema = input.getField(0).schema;
            VectorConverter.checkVectorSchema(vectorSchema, DENSE);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY));
    }

    @Override
    public List<String> getCacheFiles() {
        return ImmutableList.of(labelIndexFile + "#labelindex");
    }
}
