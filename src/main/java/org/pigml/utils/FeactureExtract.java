package org.pigml.utils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class FeactureExtract extends EvalFunc<Map<Object, Double>> {
	static final Log LOG = LogFactory.getLog(FeactureExtract.class);
	private String delimiter;
	
	public FeactureExtract() {
		this(":");
	}
	
	public FeactureExtract(String delimiter) {
		this.delimiter = delimiter;
	}
	
	@Override
	public Map<Object, Double> exec(Tuple input) throws IOException {
		if (input == null) {
            return null;
        }
		Map<Object, Double> m = new HashMap<Object, Double>(input.size());
		for (int i=0; i<input.size(); i++) {
			String[] ss = StringUtils.split(input.get(i).toString(), delimiter, 2);
			if (ss.length == 2) {
				m.put(ss[0], Double.valueOf(ss[1]));
			} else {
				LOG.error("error format "+input.get(i));
			}
		}
        return m;
	}
	
	@Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.MAP));
    }
}
