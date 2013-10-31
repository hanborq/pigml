package org.pigml.classify.naivebayes;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.UDFContext;
import org.pigml.utils.SchemaUtils;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Map a label to a index by using a label index file.
 */
public class LabelToIndex extends EvalFunc<Integer> {

    private String labelIndexPath;
    private LabelIndex labelIndex;

    public LabelToIndex(String labelIndexPath) {
        this.labelIndexPath = labelIndexPath;
    }

    @Override
    public Integer exec(Tuple input) throws IOException {
        if (labelIndex == null) {
            initializeLabelIndex();
        }

        String label = (String) input.get(0);
        return labelIndex.getIndex(label);
    }

    private void initializeLabelIndex() {
        Configuration conf = UDFContext.getUDFContext().getJobConf();
        Path liDir = new Path("file://" + new File("labelindex").getAbsoluteFile());
        labelIndex = LabelIndex.materialize(liDir, conf);
    }

    @Override
    public Schema outputSchema(Schema input) {
        Preconditions.checkArgument(input.size() == 1,
                "Expect (chararray), input " + input.size() + " fields.");
        SchemaUtils.claim(input, 0, DataType.CHARARRAY);
        return new Schema(new Schema.FieldSchema("index", DataType.INTEGER));
    }

    @Override
    public List<String> getCacheFiles() {
        return ImmutableList.of(labelIndexPath + "#labelindex");
    }
}
