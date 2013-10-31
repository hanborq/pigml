# Data Source

  http://kdd.ics.uci.edu/databases/reuters21578/reuters21578.tar.gz

## Preparations (From .tar.gz to TF/IDF vector in SequenceFile)

    WORK_DIR=/tmp/mahout-work-${USER}
    MAHOUT=${MAHOUT_HOME:-.}/bin/mahout
    curl http://kdd.ics.uci.edu/databases/reuters21578/reuters21578.tar.gz -o ${WORK_DIR}/reuters21578.tar.gz
    tar xzf ${WORK_DIR}/reuters21578.tar.gz -C ${WORK_DIR}/reuters-sgm
    $MAHOUT org.apache.lucene.benchmark.utils.ExtractReuters ${WORK_DIR}/reuters-sgm ${WORK_DIR}/reuters-out
    $HADOOP dfs -put ${WORK_DIR}/reuters-sgm ${WORK_DIR}/reuters-sgm
    $HADOOP dfs -put ${WORK_DIR}/reuters-out ${WORK_DIR}/reuters-out
    $MAHOUT seqdirectory -i ${WORK_DIR}/reuters-out -o ${WORK_DIR}/reuters-out-seqdir -c UTF-8 -chunk 5

    $MAHOUT seq2sparse \
        -i ${WORK_DIR}/reuters-out-seqdir/ \
        -o ${WORK_DIR}/reuters-out-seqdir-sparse-lda -ow --maxDFPercent 85 --namedVector

    $MAHOUT rowid \
        -i ${WORK_DIR}/reuters-out-seqdir-sparse-lda/tfidf-vectors \
        -o ${WORK_DIR}/reuters-out-matrix

# PIGML snippets

## Setup and common definitions

    set pigml.jobid 'reuter'
    set pigml.reuter.numtopics 10
    set pigml.reuter.numterms 0
    set pigml.reuter.maxiters 10
    --set pigml.reuter.dictionary '/tmp/mahout-work-hadoop/reuters-out-seqdir-sparse-lda/dictionary.file-*'
    set pigml.reuter.dictionary '/pigml/artifact/reuter/dict/part*'

    define VectorLoader org.pigml.storage.vector.SparseVectorLoader;
    define VectorStorer org.pigml.storage.vector.VectorStore;
    define RollingVectorLoader org.pigml.storage.vector.RollingVectorLoader;
    define RollingVectorStorer org.pigml.storage.vector.RollingVectorStore;
    define ModelTrainer org.pigml.cluster.lda.cvb.ModelTrain('/pigml/artifact/reuter/model');
    define ModelInference org.pigml.cluster.lda.cvb.ModelInference('/pigml/artifact/reuter/model');
    define ModelPerplexity org.pigml.cluster.lda.cvb.ModelPerplexity('/pigml/artifact/reuter/model');
    define VectorSum org.pigml.udf.vector.VectorSum;
    define VectorNorm1 org.pigml.udf.vector.VectorNorm1;
    define MaxValueIndex org.pigml.udf.vector.MaxValueIndex;
    define TupleSum org.pigml.udf.TupleSum;
    define Shuffle org.pigml.udf.Shuffle;

## Model training

    SET job.name 'lda-cvb model training'
    --corpus = load '/tmp/mahout-work-hadoop/reuters-out-matrix/matrix' using VectorLoader() as (id, vector);
    corpus = load '/pigml/artifact/reuter/rowid/matrix' using VectorLoader() as (id, vector);
    grouped = group corpus by Shuffle() % 2 PARALLEL 2;
    model = foreach grouped {
        b0 = foreach corpus generate flatten(vector);
        generate flatten(ModelTrainer(b0)) as (topicid, vector);
    }
    model = group model by topicid;
    model = foreach model {
        b0 = foreach model generate flatten(vector);
        generate group as topicid, VectorSum(b0);
    }
    store model into '/pigml/artifact/reuter/model' using RollingVectorStorer();

## Model perplexity calculate

    SET job.name 'lda-cvb model perplexity'
    --corpus = load '/tmp/mahout-work-hadoop/reuters-out-matrix/matrix' using VectorLoader() as (key, docvec);
    corpus = load '/pigml/artifact/reuter/rowid/matrix' using VectorLoader() as (key, docvec);
    samples = sample corpus 0.1;
    plex = foreach samples generate flatten(ModelPerplexity(docvec.$0, docvec.$1)) as (key, value);
    plex = group plex all;
    plex = foreach plex generate flatten(TupleSum(plex)) as (key, value);
    plex = foreach plex generate $1/$0;
    dump plex;

## Finishing model

    SET job.name 'lda-cvb model normaliz'
    model = load '/pigml/artifact/reuter/model' using RollingVectorLoader() as (key, value);
    tv = foreach model generate key, VectorNorm1(value.$0, value.$1);
    store tv into '/pigml/artifact/reuter/model/topicmodel' using VectorStorer();

## Document topic inference

    SET job.name 'lda-cvb model topic inference'
    --corpus = load '/tmp/mahout-work-hadoop/reuters-out-matrix/matrix' using VectorLoader() as (key, docvec);
    corpus = load '/pigml/artifact/reuter/rowid/matrix' using VectorLoader() as (key, docvec);
    dv = foreach corpus generate key, ModelInference(docvec.$0, docvec.$1);
    store dv into '/pigml/artifact/reuter/inference' using VectorStorer();

## Random dump

    SET job.name 'lda-cvb inference dump'
    define StringLoader org.pigml.storage.writable.WritableLoader('int', 'string');
    define Sampling datafu.pig.sampling.ReservoirSample('10');
    dv = load '/pigml/artifact/reuter/inference' using VectorLoader() as (id, vector);
    dv = foreach dv generate id, MaxValueIndex(vector.$0, vector.$1) as topic;
    --src = load '/tmp/mahout-work-hadoop/reuters-out-matrix/docIndex' using StringLoader() as (id, title);
    src = load '/pigml/artifact/reuter/rowid/docIndex' using StringLoader() as (id, title);
    joined = join dv by id, src by id;
    joined = foreach joined generate src::title as title, dv::topic as topic;
    final = group joined by topic;
    final = foreach final generate group as topic, joined.title as title;
    samples = foreach final generate topic, Sampling(title) as title;
    dump samples;

### Count num doc in each topic

    SET job.name 'lda-cvb inference dump'
    define StringLoader org.pigml.storage.writable.WritableLoader('int', 'string');
    define Sampling datafu.pig.sampling.ReservoirSample('10');
    step0 = load '/pigml/artifact/reuter/inference' using VectorLoader() as (id, vector);
    step1 = foreach step0 generate id, MaxValueIndex(vector.$0, vector.$1) as topic;
    step2 = group step1 by topic;
    step3 = foreach step2 generate group as topic, COUNT(step1) as count;
    dump step3;

## Dump Topic Keywords

    SET job.name 'lda-cvb topic keyword dump'
    define TopValueIndex org.pigml.udf.vector.TopValueIndex('50');
    define DictLoader org.pigml.storage.writable.WritableLoader('string','int');
    model = load '/pigml/artifact/reuter/model' using RollingVectorLoader() as (topic, vector);
    dict = load '/pigml/artifact/reuter/dict' using DictLoader() as (term, index);
    step1 = foreach model generate topic, flatten(TopValueIndex(vector.$0, vector.$1)) as index;
    step2 = join step1 by index, dict by index;
    step3 = foreach step2 generate step1::topic as topic, dict::term as term;
    step4 = group step3 by topic;
    step5 = foreach step4 generate group as topic, step3.term;
    dump step5;

