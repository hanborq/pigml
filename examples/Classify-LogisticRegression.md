dataset: "dataset-svmlight-classify.txt"

Model Training

define LRClassifierTrainer com.hanborq.pigml.LRClassifierTrainer('0.1');
training = load '/ml/url90' using SVMLoader();
training = foreach training generate RANDOM(), *;
training = order training by $0 parallel 2;
training = foreach training generate $1 ..;
training = foreach training generate Decode($0, '+1', 1, '-1', 0), $1;
store training into '/ml/lrmodel' using LRClassifierTrainer();

Model Verification

define Classifier com.hanborq.pigml.Classifier('/ml/lrmodel','int');
data = load '/ml/url10' using SVMLoader();
data = foreach data generate Decode($0, '+1', 1, '-1', 0) as label, Classifier($1) as prediction;
result = foreach data generate (label == prediction ? 'match' : 'mismatch') as matching;
cnt = group result by matching;
cnt = foreach cnt generate group, COUNT(result);
dump cnt;
