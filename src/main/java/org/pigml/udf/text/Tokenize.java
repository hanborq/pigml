package org.pigml.udf.text;

import com.google.common.io.Closeables;
import org.apache.hadoop.conf.Configuration;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.mahout.common.lucene.AnalyzerUtils;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.pigml.utils.Env;
import org.pigml.utils.SchemaUtils;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: g
 * Date: 9/14/13
 * Time: 2:55 PM
 * To change this template use File | Settings | File Templates.
 */
public class Tokenize extends EvalFunc<DataBag> {
    private BagFactory bfac;
    private TupleFactory tfac;
    private Analyzer analyzer;

    public Tokenize() throws IOException {
        this(StandardAnalyzer.class.getName());
    }

    public Tokenize(final String analyzerClassName) throws IOException {
        Env.inBackground(new Env.BackgroundProcedure() {
            @Override
            public void execute(Configuration conf) throws IOException {
                try {
                    analyzer = AnalyzerUtils.createAnalyzer(analyzerClassName);
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException(e);
                }
                bfac = BagFactory.getInstance();
                tfac = TupleFactory.getInstance();
            }
        });
    }

    @Override
    public DataBag exec(Tuple input) throws IOException {
        if (input == null || input.size() < 2) {
            return null;
        }
        TokenStream stream = analyzer.tokenStream(input.get(0).toString(),
                new StringReader((String) input.get(1)));
        stream.reset();
        CharTermAttribute termAtt = stream.addAttribute(CharTermAttribute.class);
        stream.reset();
        List<Tuple> tuples = new ArrayList<Tuple>();
        while (stream.incrementToken()) {
            if (termAtt.length() > 0) {
                tuples.add(tfac.newTuple(new String(termAtt.buffer(), 0, termAtt.length())));
            }
        }
        stream.end();
        Closeables.close(stream, true);
        return bfac.newDefaultBag(tuples);
    }

    @Override
    public Schema outputSchema(final Schema input) {
        SchemaUtils.claim(input, 0, DataType.CHARARRAY);
        SchemaUtils.claim(input, 1, DataType.CHARARRAY);
        try {
            Schema innerTuple = new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY));
            return new Schema(new Schema.FieldSchema(null, innerTuple, DataType.BAG));
        } catch (FrontendException e) {
            throw new RuntimeException(e);
        }
    }
}
