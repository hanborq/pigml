package org.pigml.udf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.pigml.utils.SchemaUtils;

import java.io.IOException;
import java.util.Random;

//generate non-negative random integer
public class Shuffle extends EvalFunc<Integer> {

	private final Random random = new Random();

    @Override
	public Integer exec(Tuple input) throws IOException {
        while (true) {
		    int n = Math.abs(random.nextInt());
            if (n >= 0)
                return n;
        }
	}
	
	@Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.INTEGER));
    }
}
