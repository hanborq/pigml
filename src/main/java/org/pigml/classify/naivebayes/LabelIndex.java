package org.pigml.classify.naivebayes;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.PathFilters;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterable;

import java.util.HashMap;
import java.util.Map;

/**
 * Represent a label index mapping.
 */
public class LabelIndex {

    private String DEFAULT_LABEL = "DEFAULT";

    private Map<String, Integer> labelIndexMap;
    private Map<Integer, String> indexLabelMap;
    private String defaultLabel;

    public LabelIndex(Map<String, Integer> labelIndexMap,
                      Map<Integer, String> indexLabelMap) {
        this.labelIndexMap = labelIndexMap;
        this.indexLabelMap = indexLabelMap;
        this.defaultLabel = DEFAULT_LABEL;
    }

    public int getIndex(String label) {
        return labelIndexMap.get(label);
    }

    public String getLabel(int index) {
        if (indexLabelMap.containsKey(index)) {
            return indexLabelMap.get(index);
        } else {
            return defaultLabel;
        }
    }

    public static LabelIndex materialize(Path liDir, Configuration conf) {

        Map<String, Integer> labelIndexMap = new HashMap<String, Integer>();
        Map<Integer, String> indexLabelMap = new HashMap<Integer, String>();

        SequenceFileDirIterable<Text, IntWritable> kvs =
                new SequenceFileDirIterable<Text, IntWritable>(
                        liDir, PathType.LIST, PathFilters.logsCRCFilter(), conf);
        for (Pair<Text, IntWritable> kv : kvs) {
            String label = kv.getFirst().toString();
            Integer index = kv.getSecond().get();
            labelIndexMap.put(label, index);
            indexLabelMap.put(index, label);
        }

        return new LabelIndex(labelIndexMap, indexLabelMap);
    }

}
