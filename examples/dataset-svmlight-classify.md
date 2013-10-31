Download

http://archive.ics.uci.edu/ml/machine-learning-databases/url/url_svmlight.tar.gz

Description

http://archive.ics.uci.edu/ml/datasets/URL+Reputation

Data Upload

Data Shuffle (90/10 split for training/verification)

    a = load '/ml/url_svmlight' using PigStorage(' ');
    a = foreach a generate RANDOM(), *;
    split a into training if $0 <= 0.9, test if $0 > 0.9;
    training = group training all;
    training = foreach training generate flatten($1);
    training = foreach training generate $1 ..;
    test = group test all;
    test = foreach test generate flatten($1);
    test = foreach test generate $1 ..;
    store training into '/ml/url90' using PigStorage(' ');
    store test into '/ml/url10' using PigStorage(' ');
