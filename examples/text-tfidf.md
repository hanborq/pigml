# Input Data

assumed to be {(name:chararray, terms: {(chararray)})}

# Setup

# DictGen

    %default INPUT '/pigml/dataset/reuters/tokenized-documents';
    %default JOBBASE '/pigml/artifact/reuter';
    %default DICTDIR 'dict';
    %default TOTALDIR 'total';
    %default PARALLEL '2';
    %default MINSUPPORT 2

    define DictStore org.pigml.udf.text.DictStore('2'); --$PARALLEL
    define StringsLoader org.pigml.storage.writable.StringsLoader;
    define Tokenize org.pigml.udf.text.Tokenize();
    corpus = load '/pigml/dataset/reuters/tokenized-documents' using StringsLoader() as (name, text);
    terms = foreach corpus generate flatten(text) as t;
    grouped = group terms by t parallel 2;--$PARALLEL;
    grouped = foreach grouped generate group as t, COUNT(terms) as N;
    filtered = filter grouped by N >= 2l;--$MINSUPPORT;
    store filtered into '/pigml/artifact/reuter/dict' using DictStore();
    total = group corpus all;
    total = foreach total generate COUNT(corpus);
    store total into '/pigml/artifact/reuter/total' using PigStorage();

# TF Partial Vector (Iterates.)
  --store tf count alongside doc-cardinality

    set pigml.iterover '/pigml/artifact/reuter/dict'
    define StringsLoader org.pigml.storage.writable.StringsLoader;
    define TFStore org.pigml.udf.text.TFStore;
    corpus = load '/pigml/dataset/reuters/tokenized-documents' using StringsLoader() as (name, text);
    store corpus into '/pigml/artifact/reuter/tfpartials' using TFStore();

# Merge Partial Vector

  this will translate {(int, double)} -> {(int, double)}

    define VectorLoader org.pigml.storage.vector.SparseVectorLoader('string');
    define VectorStore org.pigml.storage.vector.VectorStore('string');
    define VectorMerge org.pigml.udf.vector.VectorMerge();
    partials = load '/pigml/artifact/reuter/tfpartials' using VectorLoader() as (name, vector);
    grouped = group partials by name;
    merged = foreach grouped {
        vectors = foreach partials generate flatten(vector);
        generate group, VectorMerge(vectors);
    }
    store merged into '/pigml/artifact/reuter/tf' using VectorStore();

# DF vector

    define VectorLoader org.pigml.storage.vector.SparseVectorLoader('string');
    define DFStore org.pigml.udf.text.DFStore;
    define VectorIndices org.pigml.udf.vector.VectorIndices;
    tf = load '/pigml/artifact/reuter/tf' using VectorLoader() as (name, vector);
    df = foreach tf generate flatten(VectorIndices(vector)) as term;
    df = group df by term parallel 2;
    df = foreach df generate group as term, COUNT(df) as num;
    store df into '/pigml/artifact/reuter/df' using DFStore();

# TFIDF vector (iterates, num iter equals num of df group)

    set pigml.iterover '/pigml/artifact/reuter/df'
    set pigml.tfidftotal '/pigml/artifact/reuter/total'
    set pigml.dictdir '/pigml/artifact/reuter/dict'
    set pigml.maxdfpercent '80'
    set pigml.mindf '1'
    define VectorLoader org.pigml.storage.vector.SparseVectorLoader('string');
    define TFIDFStore org.pigml.udf.text.TFIDFStore;
    tf = load '/pigml/artifact/reuter/tf' using VectorLoader() as (name, vector);
    store tf into '/pigml/artifact/reuter/tfidfpartials' using TFIDFStore();

# Merge TFIDF partials

    define VectorLoader org.pigml.storage.vector.SparseVectorLoader('string');
    define VectorStore org.pigml.storage.vector.VectorStore('string');
    define VectorMerge org.pigml.udf.vector.VectorMerge();
    partials = load '/pigml/artifact/reuter/tfidfpartials' using VectorLoader() as (name, vector);
    grouped = group partials by name;
    merged = foreach grouped {
        vectors = foreach partials generate flatten(vector);
        generate group, VectorMerge(vectors);
    }
    store merged into '/pigml/artifact/reuter/tfidf' using VectorStore();

# ROWID

## TFIDF

    define VectorLoader org.pigml.storage.vector.SparseVectorLoader('string');
    define RowIDStore org.pigml.storage.vector.RowIDStore('2');
    tfidf = load '/pigml/artifact/reuter/tfidf' using VectorLoader() as (name, vector);
    shuffd = group tfidf by org.pigml.udf.Shuffle() % 2 parallel 2;
    store shuffd into '/pigml/artifact/reuter/rowid' using RowIDStore();

## TF (for plsi)

    define VectorLoader org.pigml.storage.vector.SparseVectorLoader('string');
    define RowIDStore org.pigml.storage.vector.RowIDStore('2');
    tfidf = load '/pigml/artifact/reuter/tf' using VectorLoader() as (name, vector);
    shuffd = group tfidf by org.pigml.udf.Shuffle() % 2 parallel 2;
    store shuffd into '/pigml/artifact/reuter/tfrowid' using RowIDStore();
