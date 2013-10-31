# Dump vector

    define VectorLoader org.pigml.storage.vector.SparseVectorLoader;
    step0 = load '$path' using VectorLoader() as (id, vector);
    dump step0;

# Count vector

    define VectorLoader org.pigml.storage.vector.SparseVectorLoader;
    step0 = load '$path' using VectorLoader() as (id, vector);
    step1 = group step0 all;
    step2 = foreach step1 generate COUNT(step0);
    dump step2;
