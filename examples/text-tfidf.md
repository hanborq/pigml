
# Dataset Preparation

## Using Mahout (From .tar.gz to TF/IDF vector in SequenceFile)

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

## Using PIGML

    #cd WORKING-DIRECTORY
    curl http://kdd.ics.uci.edu/databases/reuters21578/reuters21578.tar.gz -o reuters21578.tar.gz
    tar xzf reuters21578.tar.gz -C reuters-sgm
    mahout org.apache.lucene.benchmark.utils.ExtractReuters reuters-sgm reuters-txt

    hadoop fs -put reuters-txt /pigml/retuers/plaintxt
    mahout seqdirectory -i /pigml/reuter/plaintxt -o /pigml/reuter/seqtxt -c UTF-8 -chunk 5

# Tokenize

    define SeqTextLoader org.pigml.storage.writable.WritableLoader('string','string');
    define StringsStore org.pigml.storage.writable.StringsStore();
    text = load '/pigml/reuter/seqtxt' using SeqTextLoader() as (key, value);
    tokenized = foreach text generate key, org.pigml.udf.text.Tokenize('', value);
    store tokenized into '/pigml/reuter/tokenized' using StringsStore();

# DictGen

    %default JOBBASE '/pigml/reuter';
    %default INPUT '/pigml/reuter/tokenized';
    %default DICTDIR 'dict';
    %default TOTALDIR 'total';
    %default PARALLEL '2';
    %default MINSUPPORT 2

    define DictStore org.pigml.udf.text.DictStore('2'); --$PARALLEL
    define StringsLoader org.pigml.storage.writable.StringsLoader;
    define Tokenize org.pigml.udf.text.Tokenize();
    corpus = load '/pigml/reuter/tokenized' using StringsLoader() as (name, text);
    terms = foreach corpus generate flatten(text) as t;
    grouped = group terms by t parallel 2;--$PARALLEL;
    grouped = foreach grouped generate group as t, COUNT(terms) as N;
    filtered = filter grouped by N >= 2l;--$MINSUPPORT;
    store filtered into '/pigml/reuter/dict' using DictStore();
    total = group corpus all;
    total = foreach total generate COUNT(corpus);
    store total into '/pigml/reuter/total' using PigStorage();

# TF Partial Vector (Iterates.)
  --store tf count alongside doc-cardinality

    set pigml.iterover '/pigml/reuter/dict'
    define StringsLoader org.pigml.storage.writable.StringsLoader;
    define TFStore org.pigml.udf.text.TFStore;
    corpus = load '/pigml/reuter/tokenized' using StringsLoader() as (name, text);
    store corpus into '/pigml/reuter/tfpartials' using TFStore();

# Merge Partial Vector

  this will translate {(int, double)} -> {(int, double)}

    define VectorLoader org.pigml.storage.vector.SparseVectorLoader('string');
    define VectorStore org.pigml.storage.vector.VectorStore('string');
    define VectorMerge org.pigml.udf.vector.VectorMerge();
    partials = load '/pigml/reuter/tfpartials' using VectorLoader() as (name, vector);
    grouped = group partials by name;
    merged = foreach grouped {
        vectors = foreach partials generate flatten(vector);
        generate group, VectorMerge(vectors);
    }
    store merged into '/pigml/reuter/tf' using VectorStore();

# DF vector

    define VectorLoader org.pigml.storage.vector.SparseVectorLoader('string');
    define DFStore org.pigml.udf.text.DFStore;
    define VectorIndices org.pigml.udf.vector.VectorIndices;
    tf = load '/pigml/reuter/tf' using VectorLoader() as (name, vector);
    df = foreach tf generate flatten(VectorIndices(vector)) as term;
    df = group df by term parallel 2;
    df = foreach df generate group as term, COUNT(df) as num;
    store df into '/pigml/reuter/df' using DFStore();

# TFIDF vector (iterates, num iter equals num of df group)

    set pigml.iterover '/pigml/reuter/df'
    set pigml.tfidftotal '/pigml/reuter/total'
    set pigml.dictdir '/pigml/reuter/dict'
    set pigml.maxdfpercent '80'
    set pigml.mindf '1'
    define VectorLoader org.pigml.storage.vector.SparseVectorLoader('string');
    define TFIDFStore org.pigml.udf.text.TFIDFStore;
    tf = load '/pigml/reuter/tf' using VectorLoader() as (name, vector);
    store tf into '/pigml/reuter/tfidfpartials' using TFIDFStore();

# Merge TFIDF partials

    define VectorLoader org.pigml.storage.vector.SparseVectorLoader('string');
    define VectorStore org.pigml.storage.vector.VectorStore('string');
    define VectorMerge org.pigml.udf.vector.VectorMerge();
    partials = load '/pigml/reuter/tfidfpartials' using VectorLoader() as (name, vector);
    grouped = group partials by name;
    merged = foreach grouped {
        vectors = foreach partials generate flatten(vector);
        generate group, VectorMerge(vectors);
    }
    store merged into '/pigml/reuter/tfidf' using VectorStore();

# ROWID

## TFIDF

    define VectorLoader org.pigml.storage.vector.SparseVectorLoader('string');
    define RowIDStore org.pigml.storage.vector.RowIDStore('2');
    tfidf = load '/pigml/reuter/tfidf' using VectorLoader() as (name, vector);
    shuffd = group tfidf by org.pigml.udf.Shuffle() % 2 parallel 2;
    store shuffd into '/pigml/reuter/rowid' using RowIDStore();

## TF (for plsi)

    define VectorLoader org.pigml.storage.vector.SparseVectorLoader('string');
    define RowIDStore org.pigml.storage.vector.RowIDStore('2');
    tfidf = load '/pigml/reuter/tf' using VectorLoader() as (name, vector);
    shuffd = group tfidf by org.pigml.udf.Shuffle() % 2 parallel 2;
    store shuffd into '/pigml/reuter/tfrowid' using RowIDStore();
