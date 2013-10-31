package org.pigml.cluster.lda.cvb;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterable;
import org.apache.mahout.math.*;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.function.Functions;
import org.pigml.lang.Pair;

import java.io.IOException;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: g
 * Date: 9/14/13
 * Time: 10:31 AM
 * To change this template use File | Settings | File Templates.
 */
public class TopicModel implements Iterable<MatrixSlice> {
    private final int numTopics;
    private final Matrix topicTermCounts;
    private final Vector topicSums;
    private final double alpha;
    private final double eta;
    private final String[] dictionary;
    private final int numTerms;

    public TopicModel(Matrix topicTermCounts, Vector topicSums, double eta, double alpha,
                      String[] dictionary, double modelWeight) {
        this.dictionary = dictionary;
        this.topicTermCounts = topicTermCounts;
        this.topicSums = topicSums;
        this.numTopics = topicSums.size();
        this.numTerms = topicTermCounts.numCols();
        this.eta = eta;
        this.alpha = alpha;
        if (modelWeight != 1) {
            topicSums.assign(Functions.mult(modelWeight));
            for (int x = 0; x < numTopics; x++) {
                topicTermCounts.viewRow(x).assign(Functions.mult(modelWeight));
            }
        }
    }

    public void train(Vector original, Vector topics, Matrix docTopicModel, int iter) {
        for (int i=0; i<iter; i++) {
            trainDocTopicModel(original, topics, docTopicModel);
        }
    }

    public void update(Matrix docTopicCounts) {
        for (int topic = 0; topic < numTopics; topic++) {
            Vector v = docTopicCounts.viewRow(topic);
            topicTermCounts.viewRow(topic).assign(v, Functions.PLUS);
            topicSums.set(topic, topicSums.get(topic) + v.norm(1));
        }
    }

    /**
     * \(sum_x sum_a (c_ai * log(p(x|i) * p(a|x)))\)
     */
    public double perplexity(Vector document, Vector docTopics) {
        double perplexity = 0;
        double norm = docTopics.norm(1) + (docTopics.size() * alpha);
        for (Vector.Element e : document.nonZeroes()) {
            int term = e.index();
            double prob = 0;
            for (int x = 0; x < numTopics; x++) {
                double d = (docTopics.get(x) + alpha) / norm;
                double p = d * (topicTermCounts.viewRow(x).get(term) + eta)
                        / (topicSums.get(x) + eta * numTerms);
                prob += p;
            }
            perplexity += e.get() * Math.log(prob);
        }
        return -perplexity;
    }

    //follows private stuffs

    private void trainDocTopicModel(Vector original, Vector topics, Matrix docTopicModel) {
        // first calculate p(topic|term,document) for all terms in original, and all topics,
        // using p(term|topic) and p(topic|doc)
        pTopicGivenTerm(original, topics, docTopicModel);
        normalizeByTopic(docTopicModel);
        // now multiply, term-by-term, by the document, to propertyOf the weighted distribution of
        // term-topic pairs from this document.
        for (Vector.Element e : original.nonZeroes()) {
            for (int x = 0; x < numTopics; x++) {
                Vector docTopicModelRow = docTopicModel.viewRow(x);
                docTopicModelRow.setQuick(e.index(), docTopicModelRow.getQuick(e.index()) * e.get());
            }
        }
        // now recalculate \(p(topic|doc)\) by summing contributions from all of pTopicGivenTerm
        topics.assign(0.0);
        for (int x = 0; x < numTopics; x++) {
            topics.set(x, docTopicModel.viewRow(x).norm(1));
        }
        // now renormalize so that \(sum_x(p(x|doc))\) = 1
        topics.assign(Functions.mult(1 / topics.norm(1)));
    }

    /**
     * Computes {@code \(p(topic x | term a, document i)\)} distributions given input document {@code i}.
     * {@code \(pTGT[x][a]\)} is the (un-normalized) {@code \(p(x|a,i)\)}, or if docTopics is {@code null},
     * {@code \(p(a|x)\)} (also un-normalized).
     *
     * @param document doc-term vector encoding {@code \(w(term a|document i)\)}.
     * @param docTopics {@code docTopics[x]} is the overall weight of topic {@code x} in given
     *          document. If {@code null}, a topic weight of {@code 1.0} is used for all topics.
     * @param termTopicDist storage for output {@code \(p(x|a,i)\)} distributions.
     */
    private void pTopicGivenTerm(Vector document, Vector docTopics, Matrix termTopicDist) {
        // for each topic x
        for (int x = 0; x < numTopics; x++) {
            // propertyOf p(topic x | document i), or 1.0 if docTopics is null
            double topicWeight = docTopics == null ? 1.0 : docTopics.get(x);
            // propertyOf w(term a | topic x)
            Vector topicTermRow = topicTermCounts.viewRow(x);
            // propertyOf \sum_a w(term a | topic x)
            double topicSum = topicSums.get(x);
            // propertyOf p(topic x | term a) distribution to update
            Vector termTopicRow = termTopicDist.viewRow(x);

            // for each term a in document i with non-zero weight
            for (Vector.Element e : document.nonZeroes()) {
                int termIndex = e.index();

                // calc un-normalized p(topic x | term a, document i)
                double termTopicLikelihood = (topicTermRow.get(termIndex) + eta) * (topicWeight + alpha)
                        / (topicSum + eta * numTerms);
                termTopicRow.set(termIndex, termTopicLikelihood);
            }
        }
    }

    private void normalizeByTopic(Matrix perTopicSparseDistributions) {
        // then make sure that each of these is properly normalized by topic: sum_x(p(x|t,d)) = 1
        for (Vector.Element e : perTopicSparseDistributions.viewRow(0).nonZeroes()) {
            int a = e.index();
            double sum = 0;
            for (int x = 0; x < numTopics; x++) {
                sum += perTopicSparseDistributions.viewRow(x).get(a);
            }
            for (int x = 0; x < numTopics; x++) {
                perTopicSparseDistributions.viewRow(x).set(a,
                        perTopicSparseDistributions.viewRow(x).get(a) / sum);
            }
        }
    }

    @Override
    public Iterator<MatrixSlice> iterator() {
        return topicTermCounts.iterateAll();
    }
}
