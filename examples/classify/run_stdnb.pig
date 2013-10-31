import 'naivebayes.pig';

-- train model
corpus = load_corpus('/tmp/mahout-work-bing/20news-train-vectors');
extract_label(corpus, '/labelindex');
stdnb_train_model(corpus, '/labelindex', '/model');

-- test with a prepared corpus and print the confusion matrix
test_corpus = load_corpus('/tmp/mahout-work-bing/20news-test-vectors');
result = stdnb_test(test_corpus, '/model', '/labelindex');
printable = print_confusion_matrix(result);
dump printable;

-- classify a brand new corpus
classfied = stdnb_classify(test_corpus, '/model', '/labelindex');