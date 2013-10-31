package org.pigml.cluster.lda.cvb;

/**
 * Created with IntelliJ IDEA.
 * User: g
 * Date: 9/17/13
 * Time: 3:37 PM
 * To change this template use File | Settings | File Templates.
 */
public interface Constants {

    String NUM_TOPICS = "numtopics";
    String NUM_TERMS = "numterms";
    String DOC_TOPIC_SMOOTHING = "doctopicsmoothing";
    String TERM_TOPIC_SMOOTHING = "termtopicsmoothing";
    String DICTIONARY = "dictionary";
    String MAX_ITERATIONS_PER_DOC = "maxdoctopiciters";
    String MODEL_WEIGHT = "previtermult";

    double DEFAULT_DOC_TOPIC_SMOOTHING = 0.0001;
    double DEFAULT_TERM_TOPIC_SMOOTHING = 0.0001;
    int DEFAULT_MAX_ITERATIONS_PER_DOC = 10;
    float DEFAULT_MODEL_WEIGHT = 1.0f;
    float DEFAULT_TEST_SET_FRACTION = 0.1f;
}
