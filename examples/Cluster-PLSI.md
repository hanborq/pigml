## Corpus spliting
  split vertically in the feature space

    SET job.name 'plsi corpus splitting'
    define VectorLoader org.pigml.storage.vector.SparseVectorLoader;
    define VectorSplit org.pigml.udf.vector.VectorSplit('200');
    define GroupedVectorStore org.pigml.storage.vector.GroupedVectorStore;
    step0 = load '/pigml/artifact/reuter/tfrowid/matrix' using VectorLoader() as (id, vector);
    step1 = foreach step0 generate id, flatten(VectorSplit(vector.$0, vector.$1)) as (part, vector);
    step2 = group step1 by part;
    step3 = foreach step2 {
        b0 = foreach step1 generate id, vector;
        generate group as part, b0;
    }
    store step3 into '/pigml/artifact/reuter/tfrowid/parted' using GroupedVectorStore();

## Sum TF to get Sigma(d, w)

    SET job.name 'sigma(d,w)'
    define VectorLoader org.pigml.storage.vector.SparseVectorLoader;
    step0 = load '/pigml/artifact/reuter/tfrowid/matrix' using VectorLoader() as (id, vector);
    step1 = foreach step0 generate org.pigml.udf.vector.VectorZSum(vector) as zsum;
    step2 = group step1 all;
    step3 = foreach step2 generate SUM(step1.zsum);
    dump step3;

#symmetric
## Model training
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    SET job.name 'plsi sym model training'
    define PLSITrain org.pigml.cluster.plsi.sym.PLSITrain('5', '21670', '41900', '200');
    define VectorLoader org.pigml.storage.vector.SparseVectorLoader;
    corpus = load '/pigml/artifact/reuter/tfrowid/parted' using VectorLoader() as (id, vector);
    store corpus into '/pigml/artifact/reuter/plsi' using PLSITrain();

## Model likelihood
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    SET job.name 'plsi model update'
    define PLSILikelihood org.pigml.cluster.plsi.sym.PLSILikelihood;
    define VectorLoader org.pigml.storage.vector.SparseVectorLoader;
    corpus = load '/pigml/artifact/reuter/tfrowid/matrix' using VectorLoader() as (id, vector);
    store corpus into '/pigml/artifact/reuter/plsi' using PLSILikelihood();

# Asymmetric
## Model training

    SET job.name 'plsi model training'
    define PLSITrain org.pigml.cluster.plsi.asym.PLSITrain('5', '21670', '41900', '200');
    define VectorLoader org.pigml.storage.vector.SparseVectorLoader;
    corpus = load '/pigml/artifact/reuter/tfrowid/parted' using VectorLoader() as (id, vector);
    store corpus into '/pigml/artifact/reuter/plsi' using PLSITrain();

## Model likelihood

    SET job.name 'plsi model update'
    define PLSILikelihood org.pigml.cluster.plsi.asym.PLSILikelihood;
    define VectorLoader org.pigml.storage.vector.SparseVectorLoader;
    corpus = load '/pigml/artifact/reuter/tfrowid/matrix' using VectorLoader() as (id, vector);
    store corpus into '/pigml/artifact/reuter/plsi' using PLSILikelihood();

# new Symmetric

    SET job.name 'plsi model training'
    --SET pig.noSplitCombination true
    define PLSIPzdw org.pigml.cluster.plsi.asym.PLSIPzdw('/pigml/artifact/reuter/plsi', '5', '21670', '41900', '200');
    define VectorLoader org.pigml.storage.vector.SparseVectorLoader;
    corpus = load '/pigml/artifact/reuter/tfrowid/parted' using VectorLoader() as (id, vector);
    pzdw = foreach corpus generate flatten(PLSIPzdw(id, vector));
    pzd = group pzdw by d;
    pwz = group pzdw by w;
    store pzd into '/pigml/artifact/reuter/plsi/pzd' using PLSIStore();
    store pwz into '/pigml/artifact/reuter/plsi/pwz' using PLSIStore();
