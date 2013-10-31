
-- load corpus from files
DEFINE load_corpus(path)
RETURNS corpus {
    DEFINE VectorLoader org.pigml.storage.vector.SparseVectorLoader('CHARARRAY');
    $corpus = LOAD '$path' USING VectorLoader() AS (id, vector);
    -- corpus => {t:(id:chararray, vector:(cardinality:int, vector:{index:int, value:double}))}
};

-- extract labels for a corpus and store INTO outdir
DEFINE extract_label(corpus, outdir)
RETURNS void {
    DEFINE Label org.pigml.classify.naivebayes.Label;
    DEFINE IndexLabels org.pigml.classify.naivebayes.IndexLabels;

    labels = FOREACH $corpus GENERATE Label(id) AS label;
    -- uniqed = DISTINCT labels;
    -- ranked = RANK uniqed BY label ASC;
    -- $labelindex = FOREACH ranked GENERATE label, (int)$0 - 1 AS index;
    grpd = GROUP labels ALL;
    labelindex = FOREACH grpd {
        idxed = IndexLabels(labels); 
        GENERATE FLATTEN(idxed) AS (label, index);
    };
    STORE labelindex INTO '$outdir' USING org.pigml.storage.writable.WritableStore('text', 'int');
};

-- train standard naive bayes model for a corpus and store the model INTO outdir
DEFINE stdnb_train_model(corpus, labelindex, outdir)
RETURNS void {
    DEFINE VectorStore org.pigml.storage.vector.VectorStore();
    DEFINE VectorSum org.pigml.udf.vector.VectorSum;
    DEFINE VectorZSum org.pigml.udf.vector.VectorZSum;
    DEFINE VectorDivide org.pigml.udf.vector.VectorDivide;
    DEFINE VectorPlus org.pigml.udf.vector.VectorPlus;
    DEFINE VectorDot org.pigml.udf.vector.VectorDot;
    DEFINE LabelToIndex org.pigml.classify.naivebayes.LabelToIndex('$labelindex');

    --{{ summed corpus BY label and also map label to index value
    idxed = FOREACH $corpus GENERATE LabelToIndex(Label(id)) as index, vector;
    grpd = GROUP idxed BY index;
    lf_weights = FOREACH grpd {
        vectors = FOREACH idxed GENERATE flatten(vector);
        GENERATE group AS label, VectorSum(vectors) AS weights;
    };
    STORE lf_weights INTO '$outdir/label_feature_weights' USING VectorStore;
    --}} lf_weights => {t:(label:int, weights:(cardinality:int, vector:{t:(index:int, value:double)}))}

    --{{ calcuate weights per label
    l_weights = FOREACH lf_weights GENERATE label, VectorZSum(weights) AS weight;
    STORE l_weights INTO '$outdir/label_weights' USING org.pigml.storage.writable.WritableStore('int', 'double');
    --}} l_weights => {label:int, weight:double}

    --{{ calcuate weights per feature
    grpd = GROUP lf_weights ALL;
    vsum = FOREACH grpd {
        flattened = FOREACH lf_weights GENERATE flatten(weights);
        GENERATE VectorSum(flattened) AS weights;
    }
    f_weights = FOREACH vsum GENERATE flatten(weights.vector) AS (feature, weight);
    STORE f_weights INTO '$outdir/feature_weights' USING org.pigml.storage.writable.WritableStore('int', 'double');
    --}} f_weights => {feature:int, weight:double}
};

-- classify a corpus
DEFINE stdnb_classify(corpus, model, labelindex)
RETURNS classfied {
    DEFINE BestScoredLabel org.pigml.classify.naivebayes.BestScoredLabel('$labelindex');
    DEFINE LabelScores org.pigml.classify.naivebayes.LabelScores('$model');

    $classfied = FOREACH $corpus GENERATE id, BestScoredLabel(LabelScores(vector)) AS class;
    -- => {id:chararray, class:chararray}
};

-- test corpus with standard naive bayes method with a trained model
DEFINE stdnb_test(corpus, model, labelindex)
RETURNS result {
    DEFINE BestScoredLabel org.pigml.classify.naivebayes.BestScoredLabel('$labelindex');
    DEFINE LabelScores org.pigml.classify.naivebayes.LabelScores('$model');
    DEFINE Label org.pigml.classify.naivebayes.Label;

    --{{ map corpus id to its label, and calcuate a score for it to each possible label
    classfied = FOREACH $corpus GENERATE id, BestScoredLabel(LabelScores(vector)) AS class;
    --}}

    --{{ calculate the confusion matrix
    grpd = GROUP classfied by (Label(id), class);
    counted = FOREACH grpd GENERATE FLATTEN(group) as (actual, class), COUNT(classfied) as count;
    grpd = GROUP counted BY actual;
    $result = FOREACH grpd {
        prjed = FOREACH counted GENERATE class, count;
        GENERATE group as actual, prjed as counts;
    };
    --}} result => {actual:chararray, counts:{(class:chararray, count:long)}}
};

-- print test result as confusion matrix
DEFINE print_confusion_matrix(matrix)
RETURNS printable {
    DEFINE PrintConfusionMatrix org.pigml.classify.naivebayes.PrintConfusionMatrix;

    grpd = GROUP $matrix ALL;
    $printable = FOREACH grpd GENERATE PrintConfusionMatrix($matrix);
};
