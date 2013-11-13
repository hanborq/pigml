package org.pigml.udf.text;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.pigml.utils.Env;
import org.pigml.utils.SchemaUtils;

import java.io.IOException;
import java.util.Arrays;

public class JsonFlatten extends EvalFunc<Tuple> {

    private static final Log LOG = LogFactory.getLog(JsonFlatten.class);

    private final TupleFactory tfac = TupleFactory.getInstance();
    private final String[][] paths;
    private final String[] types;
    private final ObjectMapper mapper;
    private TreeReader reader;

    public JsonFlatten(String columns) throws IOException {
        String[] array = StringUtils.split(columns, ",");
        this.paths = new String[array.length][];
        types = new String[array.length];
        for (int i=0; i<array.length; i++) {
            String [] arr = StringUtils.split(array[i], ":");
            this.paths[i] = StringUtils.split(arr[0], ".");
            types[i] = arr.length > 1 ? arr[1] : null;
        }
        this.mapper = new ObjectMapper();
        Env.inBackground(new Env.BackgroundProcedure() {
            @Override
            public void execute(Configuration conf) throws IOException {
                byte type = Byte.parseByte(Env.getProperty(JsonFlatten.class, "input.type"));
                Preconditions.checkState(type == DataType.CHARARRAY || type == DataType.BYTEARRAY);
                if (type == DataType.CHARARRAY) {
                    reader = new TreeReader() {
                        @Override
                        public JsonNode read(Tuple tuple) throws IOException {
                            return mapper.readTree((String) tuple.get(0));
                        }
                    };
                } else {
                    reader = new TreeReader() {
                        @Override
                        public JsonNode read(Tuple tuple) throws IOException {
                            DataByteArray dba = (DataByteArray) tuple.get(0);
                            return dba != null ? mapper.readTree(dba.get()) : null;
                        }
                    };
                }
            }
        });
    }

    @Override
    public Tuple exec(Tuple input) throws IOException {
        if (input != null && input.size() > 0) {
            final JsonNode tree = reader.read(input);
            if (tree != null) {
                int index=0;
                String[] results = new String[types.length];
                for (String[] path : paths) {
                    JsonNode node = tree;
                    for (int i=0; i<path.length && node != null; i++) {
                        node = node.get(path[i]);
                    }
                    if (node != null) {
                        results[index++] = node.asText();
                    }
                }
                return tfac.newTupleNoCopy(Arrays.asList(results));
            }
        }
        return null;
    }

    @Override
    public Schema outputSchema(Schema input) {
        Schema.FieldSchema valid = SchemaUtils.claim(
                input, 0, DataType.CHARARRAY, DataType.BYTEARRAY);
        if (input.size() > 1) {
            LOG.warn("Extra fields after "+valid.alias+" are ignored");
        }
        Env.setProperty(JsonFlatten.class, "input.type", String.valueOf(valid.type));
        Schema.FieldSchema[] fields = new Schema.FieldSchema[paths.length];
        for (int i=0; i<fields.length; i++) {
            fields[i] = SchemaUtils.primitiveOf(DataType.CHARARRAY);
        }
        return SchemaUtils.schemaOf(fields);
    }

    private interface TreeReader {
        JsonNode read(Tuple tuple) throws IOException;
    }
}
