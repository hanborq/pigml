package org.pigml.udf;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.UDFContext;

public class ID extends EvalFunc<Long> {
	static final Log LOG = LogFactory.getLog(ID.class);
	private Long base;
	private long counter;
	@Override
	public Long exec(Tuple input) throws IOException {
		if (input == null) {
			counter++;
			return null;
		}
		if (base == null) {
			TaskAttemptID attempt = TaskAttemptID.forName(UDFContext.getUDFContext().getJobConf().get("mapred.task.id"));
			base = Long.reverse(attempt.getId() << 1);
		}
		return base + counter++;
	}
	
	@Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.LONG));
    }
}
