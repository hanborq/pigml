Dataset
=======

 See dataset-svmlight-classify

Model Training
==============

    define NBClassifierTrainer com.hanborq.pigml.NBClassifierTrainer;
    set mapred.min.split.size 4000000000;
    data = load '/ml/url90' using SVMLoader();
    data = foreach data generate Decode($0, '+1', 1, '-1', 0), $1;
    store data into '/ml/nbmodel' using NBClassifierTrainer();

Model Verification
==================

    define Classifier com.hanborq.pigml.Classifier('/ml/nbmodel','int');
    a = load '/ml/url10' using SVMLoader();
    a = foreach a generate Decode($0, '+1', 1, '-1', 0) as label, Classifier($1) as prediction;
    result = foreach a generate (label == prediction ? 'match' : 'mismatch') as matching;
    cnt = group result by matching;
    cnt = foreach cnt generate group, COUNT(result);
    dump cnt;

