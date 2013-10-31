/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.pigml.classify.naivebayes;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.PathFilters;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterable;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.SparseRowMatrix;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.map.OpenIntDoubleHashMap;

import java.io.IOException;

/**
 * NaiveBayesModel holds the weight Matrix, the feature and label sums and
 * the weight normalizer vectors. (Most logic copied from mahout.)
 */
public class NaiveBayesModel {

    private final OpenIntDoubleHashMap weightsPerLabel;
    private final OpenIntDoubleHashMap weightsPerFeature;
    private final Matrix weightsPerLabelAndFeature;
    private final float alphaI;
    private final double numFeatures;
    private final double totalWeightSum;

    public NaiveBayesModel(Matrix weightMatrix,
                           OpenIntDoubleHashMap weightsPerFeature,
                           OpenIntDoubleHashMap weightsPerLabel,
                           float alphaI) {
        this.weightsPerLabelAndFeature = weightMatrix;
        this.weightsPerFeature = weightsPerFeature;
        this.weightsPerLabel = weightsPerLabel;
        this.numFeatures = weightsPerFeature.size();
        double totalWeightSum = 0;
        for (double v : weightsPerLabel.values().elements()) {
            totalWeightSum += v;
        }
        this.totalWeightSum = totalWeightSum;
        this.alphaI = alphaI;
    }

    public double labelWeight(int label) {
        return weightsPerLabel.get(label);
    }

    public double featureWeight(int feature) {
        return weightsPerFeature.get(feature);
    }

    public double weight(int label, int feature) {
        return weightsPerLabelAndFeature.getQuick(label, feature);
    }

    public float alphaI() {
        return alphaI;
    }

    public double numFeatures() {
        return numFeatures;
    }

    public double totalWeightSum() {
        return totalWeightSum;
    }

    public int numLabels() {
        return weightsPerLabel.size();
    }

    public static NaiveBayesModel materialize(Path modelDir, Configuration conf) throws IOException {
        OpenIntDoubleHashMap weightsPerLabel = new OpenIntDoubleHashMap();
        OpenIntDoubleHashMap weightsPerFeature = new OpenIntDoubleHashMap();

        SequenceFileDirIterable<IntWritable, DoubleWritable> kvs;
        kvs = new SequenceFileDirIterable<IntWritable, DoubleWritable>(
                new Path(modelDir, "label_weights"), PathType.LIST, PathFilters.logsCRCFilter(), conf);
        for (Pair<IntWritable, DoubleWritable> kv : kvs) {
            weightsPerLabel.put(kv.getFirst().get(), kv.getSecond().get());
        }

        kvs = new SequenceFileDirIterable<IntWritable, DoubleWritable>(
                new Path(modelDir, "feature_weights"), PathType.LIST, PathFilters.logsCRCFilter(), conf);
        for (Pair<IntWritable, DoubleWritable> kv : kvs) {
            weightsPerFeature.put(kv.getFirst().get(), kv.getSecond().get());
        }

        Matrix weightsPerLabelAndFeature = null;
        SequenceFileDirIterable<IntWritable, VectorWritable> labelVectors =
                new SequenceFileDirIterable<IntWritable, VectorWritable>(
                        new Path(modelDir, "label_feature_weights"), PathType.LIST, PathFilters.logsCRCFilter(), conf);
        for (Pair<IntWritable, VectorWritable> labelVector : labelVectors) {
            int label = labelVector.getFirst().get();
            Vector vector = labelVector.getSecond().get();
            if (weightsPerLabelAndFeature == null) {
                weightsPerLabelAndFeature = new SparseRowMatrix(weightsPerLabel.size(), vector.size());
            }
            weightsPerLabelAndFeature.assignRow(label, vector);
        }

        // TODO alphaI is hard-coded to 1.0
        // TODO perLabelThetaNormalizer is not supported yet
        NaiveBayesModel model = new NaiveBayesModel(
                weightsPerLabelAndFeature, weightsPerFeature, weightsPerLabel,
                1.0f);
        model.validate();
        return model;
    }

    public void validate() {
        Preconditions.checkState(alphaI > 0, "alphaI has to be greater than 0!");
        Preconditions.checkArgument(numFeatures > 0, "the vocab count has to be greater than 0!");
        Preconditions.checkArgument(totalWeightSum > 0, "the totalWeightSum has to be greater than 0!");
        Preconditions.checkArgument(weightsPerLabel != null, "the number of labels has to be defined!");
        Preconditions.checkArgument(weightsPerLabel.size() > 0,
                "the number of labels has to be greater than 0!");
        // Preconditions.checkArgument(perlabelThetaNormalizer != null, "the theta normalizers have to be defined");
        // Preconditions.checkArgument(perlabelThetaNormalizer.getNumNondefaultElements() > 0,
        //    "the number of theta normalizers has to be greater than 0!");
        Preconditions.checkArgument(weightsPerFeature != null, "the feature sums have to be defined");
        Preconditions.checkArgument(weightsPerFeature.size() > 0,
                "the feature sums have to be greater than 0!");
        // Check if all thetas have same sign.
        /*Iterator<Element> it = perlabelThetaNormalizer.iterateNonZero();
        while (it.hasNext()) {
          Element e = it.next();
          Preconditions.checkArgument(Math.signum(e.get()) == Math.signum(minThetaNormalizer), e.get()
              + "  " + minThetaNormalizer);
        }*/
    }
}
