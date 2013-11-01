## Setup and common definitions

    set pigml.jobid 'reuter'
    set pigml.reuter.numtopics 10
    set pigml.reuter.numterms 0
    set pigml.reuter.maxiters 10
    set pigml.reuter.dictionary '/pigml/reuter/dict/part*'
    define VectorLoader org.pigml.storage.vector.SparseVectorLoader;
    define VectorStorer org.pigml.storage.vector.VectorStore;
    define RollingVectorLoader org.pigml.storage.vector.RollingVectorLoader;
    define RollingVectorStorer org.pigml.storage.vector.RollingVectorStore;
    define ModelTrainer org.pigml.cluster.lda.cvb.ModelTrain('/pigml/reuter/model');
    define ModelPerplexity org.pigml.cluster.lda.cvb.ModelPerplexity('/pigml/reuter/model');
    define VectorNorm1 org.pigml.udf.vector.VectorNorm1;
    define MaxValueIndex org.pigml.udf.vector.MaxValueIndex;

## Model training

    SET job.name 'lda-cvb model training'
    corpus = load '/pigml/reuter/rowid/matrix' using VectorLoader() as (id, vector);
    grouped = group corpus by org.pigml.udf.Shuffle() % 2 PARALLEL 2;
    model = foreach grouped {
        b0 = foreach corpus generate flatten(vector);
        generate flatten(ModelTrainer(b0)) as (topicid, vector);
    }
    model = group model by topicid;
    model = foreach model {
        b0 = foreach model generate flatten(vector);
        generate group as topicid, org.pigml.udf.vector.VectorSum(b0);
    }
    store model into '/pigml/reuter/model' using RollingVectorStorer();

## Model perplexity evaluate

    SET job.name 'lda-cvb model perplexity'
    corpus = load '/pigml/reuter/rowid/matrix' using VectorLoader() as (key, docvec);
    samples = sample corpus 0.1;
    plex = foreach samples generate flatten(ModelPerplexity(docvec.$0, docvec.$1)) as (key, value);
    plex = group plex all;
    plex = foreach plex generate flatten(org.pigml.udf.TupleSum(plex)) as (key, value);
    plex = foreach plex generate $1/$0;
    dump plex;

## Finishing model

    SET job.name 'lda-cvb model normaliz'
    model = load '/pigml/reuter/model' using RollingVectorLoader() as (key, value);
    tv = foreach model generate key, VectorNorm1(value.$0, value.$1);
    store tv into '/pigml/reuter/model/final' using VectorStorer();

## Document topic inference

    SET job.name 'lda-cvb model topic inference'
    define ModelInference org.pigml.cluster.lda.cvb.ModelInference('/pigml/reuter/model/final');
    corpus = load '/pigml/reuter/rowid/matrix' using VectorLoader() as (key, docvec);
    dv = foreach corpus generate key, ModelInference(docvec.$0, docvec.$1);
    store dv into '/pigml/reuter/pkd' using VectorStorer();

## Random dump

    SET job.name 'lda-cvb inference dump'
    define StringLoader org.pigml.storage.writable.WritableLoader('int', 'string');
    define Sampling datafu.pig.sampling.ReservoirSample('10');
    dv = load '/pigml/reuter/pkd' using VectorLoader() as (id, vector);
    dv = foreach dv generate id, MaxValueIndex(vector.$0, vector.$1) as topic;
    src = load '/pigml/reuter/rowid/docIndex' using StringLoader() as (id, title);
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
    step0 = load '/pigml/reuter/pkd' using VectorLoader() as (id, vector);
    step1 = foreach step0 generate id, MaxValueIndex(vector.$0, vector.$1) as topic;
    step2 = group step1 by topic;
    step3 = foreach step2 generate group as topic, COUNT(step1) as count;
    dump step3;

## Dump Topic Keywords

    SET job.name 'lda-cvb topic keyword dump'
    define TopValueIndex org.pigml.udf.vector.TopValueIndex('50');
    define DictLoader org.pigml.storage.writable.WritableLoader('string','int');
    model = load '/pigml/reuter/model/final' using VectorLoader() as (topic, vector);
    dict = load '/pigml/reuter/dict' using DictLoader() as (term, index);
    step1 = foreach model generate topic, flatten(TopValueIndex(vector.$0, vector.$1)) as index;
    step2 = join step1 by index, dict by index;
    step3 = foreach step2 generate step1::topic as topic, dict::term as term;
    step4 = group step3 by topic;
    step5 = foreach step4 generate group as topic, step3.term;
    dump step5;

