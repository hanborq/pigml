package org.pigml.cluster.lda.cvb;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.common.iterator.sequencefile.PathFilters;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterable;
import org.apache.mahout.math.*;
import org.pigml.common.Defines;
import org.pigml.lang.Pair;
import org.pigml.utils.IOUtils;
import org.pigml.utils.IterationUtils;
import org.pigml.utils.PathUtils;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;

/**
 * Created with IntelliJ IDEA.
 * User: g
 * Date: 9/18/13
 * Time: 9:14 AM
 * To change this template use File | Settings | File Templates.
 */
public class ModelUtils implements Defines {
    private static final Log LOG = LogFactory.getLog(ModelUtils.class);

    static Callable<Pair<Matrix, Vector>> fLoadModel(final Configuration conf, final String modelloc) throws IOException {
        return fLoadModel(conf, modelloc, -1);
    }
    static Callable<Pair<Matrix, Vector>> fLoadModel(final Configuration conf, final String modelloc, final int iter)
            throws IOException {
        return new Callable<Pair<Matrix, Vector>>() {
            @Override
            public Pair<Matrix, Vector> call() throws Exception {
                LOG.info("Try loading model");
                Path home = new Path(modelloc);
                int N = iter >= 0 ? iter :
                     IterationUtils.getLastIteration(conf, new Path(modelloc));
                if (N >= 0) {
                    home = PathUtils.enter(home, String.valueOf(N));
                } else if (PathUtils.countParts(conf, modelloc) <= 0 ) {
                    LOG.info("No module found");
                    return null;
                }
                LOG.info("Loading previous model from "+home);
                int numTopics = -1;
                int numTerms = -1;
                FileStatus[] modelPaths = FileSystem.get(conf).listStatus(home, PathFilters.partFilter());
                List<org.apache.mahout.common.Pair<Integer, Vector>> rows = Lists.newArrayList();
                for (FileStatus modelPath : modelPaths) {
                    for (org.apache.mahout.common.Pair<IntWritable, VectorWritable> row
                            : new SequenceFileIterable<IntWritable, VectorWritable>(modelPath.getPath(), true, conf)) {
                        rows.add(org.apache.mahout.common.Pair.of(row.getFirst().get(), row.getSecond().get()));
                        numTopics = Math.max(numTopics, row.getFirst().get());
                        if (numTerms < 0) {
                            numTerms = row.getSecond().get().size();
                        }
                    }
                }
                if (rows.isEmpty()) {
                    throw new IOException("No vectors in "+home);
                }
                numTopics++;
                Matrix model = new DenseMatrix(numTopics, numTerms);
                Vector topicSums = new DenseVector(numTopics);
                for (org.apache.mahout.common.Pair<Integer, Vector> pair : rows) {
                    model.viewRow(pair.getFirst()).assign(pair.getSecond());
                    topicSums.set(pair.getFirst(), pair.getSecond().norm(1));
                }
                return Pair.of(model, topicSums);
            }
        };
    }

    static Callable<Pair<Matrix,Vector>> fRandomModel(final int numTopics, final int numTerms) {
        return new Callable<Pair<Matrix, Vector>>() {
            @Override
            public Pair<Matrix, Vector> call() throws Exception {
                LOG.info("Randomizing model of "+numTopics +" X "+numTerms);
                Matrix topicTermCounts = new DenseMatrix(numTopics, numTerms);
                Vector topicSums = new DenseVector(numTopics);
                Random random = new Random();
                for (int x = 0; x < numTopics; x++) {
                    for (int term = 0; term < numTerms; term++) {
                        topicTermCounts.viewRow(x).set(term, random.nextDouble());
                    }
                }
                for (int x = 0; x < numTopics; x++) {
                    topicSums.set(x, topicTermCounts.viewRow(x).norm(1));
                }
                return Pair.of(topicTermCounts, topicSums);
            }
        };
    }

    static public Callable<Integer> fCountTerms(final Configuration conf, final String location) {
        return new Callable<Integer>(){
            @Override
            public Integer call() throws Exception {
                Preconditions.checkArgument(StringUtils.isNotBlank(location), "Invalid dictionary path");
                Path dictionaryPath = new Path(location);
                FileSystem fs = dictionaryPath.getFileSystem(conf);
                Text key = new Text();
                IntWritable value = new IntWritable();
                int maxTermId = -1;
                for (FileStatus stat : fs.globStatus(dictionaryPath)) {
                    SequenceFile.Reader reader = new SequenceFile.Reader(fs, stat.getPath(), conf);
                    while (reader.next(key, value)) {
                        maxTermId = Math.max(maxTermId, value.get());
                    }
                }
                LOG.info("Counted " + (maxTermId+1) + " from dict " + location);
                return maxTermId + 1;
            }
        };
    }
}
