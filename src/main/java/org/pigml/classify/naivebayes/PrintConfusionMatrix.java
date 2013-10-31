package org.pigml.classify.naivebayes;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.pigml.utils.SchemaUtils;

import java.io.IOException;
import java.util.*;

/**
 * Pretty print the confusion matrix.
 */
public class PrintConfusionMatrix extends EvalFunc<String> {

    private static final String DEFAULT_LABEL = "default";

    private String defaultLabel;

    private Map<String, Integer> labelMap = new LinkedHashMap<String, Integer>();

    private Map<String, Map<String, Integer>> matrix = new HashMap<String, Map<String, Integer>>();


    public PrintConfusionMatrix() {
        this.defaultLabel = DEFAULT_LABEL;
    }

    @Override
    public String exec(Tuple input) throws IOException {
        DataBag bag = (DataBag) input.get(0);

        for (Tuple tuple : bag) {
            Map<String, Integer> map = new HashMap<String, Integer>();
            String label = (String) tuple.get(0);
            DataBag predicts = (DataBag) tuple.get(1);
            for (Tuple predict : predicts) {
                String lab = (String) predict.get(0);
                Long count = (Long) predict.get(1);
                map.put(lab, count.intValue());
            }
            matrix.put(label, map);
        }


        // init labelMap
        List<String> labels = new ArrayList<String>(matrix.keySet());
        Collections.sort(labels);
        int i = 0;
        for (String label : labels) {
            labelMap.put(label, i++);
        }
        labelMap.put(defaultLabel, i);

        return print();
    }

    private int getTotal(String label) {
        if (!matrix.containsKey(label)) {
            return 0;
        }
        int total = 0;
        for (Map.Entry<String, Integer> entry : matrix.get(label).entrySet()) {
            total += entry.getValue();
        }
        return total;
    }

    private int getCount(String correctLabel, String classifiedLabel) {
        if (!matrix.containsKey(correctLabel)) {
            return 0;
        }
        Map<String, Integer> vector = matrix.get(correctLabel);
        if (!vector.containsKey(classifiedLabel)) {
            return 0;
        }
        return vector.get(classifiedLabel);
    }

    public String print() {
        StringBuilder returnString = new StringBuilder(200);
        returnString.append("=======================================================").append('\n');
        returnString.append("Confusion Matrix\n");
        returnString.append("-------------------------------------------------------").append('\n');

        int unclassified = getTotal(defaultLabel);
        for (Map.Entry<String,Integer> entry : this.labelMap.entrySet()) {
            if (entry.getKey().equals(defaultLabel) && unclassified == 0) {
                continue;
            }
            returnString.append(StringUtils.rightPad(getSmallLabel(entry.getValue()), 5)).append('\t');
        }

        returnString.append("<--Classified as").append('\n');
        for (Map.Entry<String,Integer> entry : this.labelMap.entrySet()) {
            if (entry.getKey().equals(defaultLabel) && unclassified == 0) {
                continue;
            }
            String correctLabel = entry.getKey();
            int labelTotal = 0;
            for (String classifiedLabel : this.labelMap.keySet()) {
                if (classifiedLabel.equals(defaultLabel) && unclassified == 0) {
                    continue;
                }
                returnString.append(
                        StringUtils.rightPad(Integer.toString(getCount(correctLabel, classifiedLabel)), 5)).append('\t');
                labelTotal += getCount(correctLabel, classifiedLabel);
            }
            returnString.append(" |  ").append(StringUtils.rightPad(String.valueOf(labelTotal), 6)).append('\t')
                    .append(StringUtils.rightPad(getSmallLabel(entry.getValue()), 5))
                    .append(" = ").append(correctLabel).append('\n');
        }
        if (unclassified > 0) {
            returnString.append("Default Category: ").append(defaultLabel).append(": ").append(unclassified).append('\n');
        }
        returnString.append('\n');
        return returnString.toString();
    }

    static String getSmallLabel(int i) {
        int val = i;
        StringBuilder returnString = new StringBuilder();
        do {
            int n = val % 26;
            returnString.insert(0, (char) ('a' + n));
            val /= 26;
        } while (val > 0);
        return returnString.toString();
    }

    @Override
    public Schema outputSchema(Schema input) {
        Preconditions.checkArgument(input.size() == 1,
                "Expect ({chararray, {(chararray, long)}}), input " + input.size() + " fields.");

        try {
            SchemaUtils.claim(input, 0, DataType.BAG);
            Schema matrixSchema = input.getField(0).schema.getField(0).schema;
            SchemaUtils.claim(matrixSchema, 0, DataType.CHARARRAY);
            SchemaUtils.claim(matrixSchema, 1, DataType.BAG);
            Schema predictsSchema = matrixSchema.getField(1).schema;
            SchemaUtils.claim(predictsSchema, 0, DataType.TUPLE);
            Schema tupleSchema = predictsSchema.getField(0).schema;
            SchemaUtils.claim(tupleSchema, 0, DataType.CHARARRAY);
            SchemaUtils.claim(tupleSchema, 1, DataType.LONG);
        } catch (FrontendException e) {
            throw new RuntimeException(e);
        }

        return new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY));
    }
}
