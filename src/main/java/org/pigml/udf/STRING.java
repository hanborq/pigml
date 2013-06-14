package org.pigml.udf;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class STRING extends EvalFunc<String> {
	static final Log LOG = LogFactory.getLog(STRING.class);
	
	@Override
	public String exec(Tuple input) throws IOException {
		if (input == null || input.size() != 1) {
            return null;
        }
		final Object x = input.get(0);
		return x == null ? null : x.toString();
	}
	
	@Override
    public Schema outputSchema(Schema input) {
		byte t;
		try {
			t = input.getField(input.size()-1).type;
		} catch (FrontendException e) {
			LOG.error("", e);
			t = DataType.UNKNOWN;
		}
		return new Schema(new Schema.FieldSchema(null, t));
    }
}
