package org.pigml.storage.vector;

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.pig.ResourceSchema;
import org.apache.pig.data.*;
import org.apache.pig.impl.util.UDFContext;
import org.pigml.storage.AbstractStore;
import org.pigml.utils.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: g
 * Date: 13-10-12
 * Time: 下午3:17
 * To change this template use File | Settings | File Templates.
 */
public class GroupedVectorStore extends AbstractStore {

    private static final Log LOG = LogFactory.getLog(GroupedVectorStore.class);

    private Map<Object,SequenceFile.Writer> writers;
    private VectorUtils.Tuple2Vector tvgen;
    private IntWritable keyWritable;
    private VectorWritable valueWritable;

    public GroupedVectorStore() throws IOException {
        super(new NullOutputFormat());
        Env.inBackground(new Env.BackgroundProcedure() {
            @Override
            public void execute(Configuration conf) throws IOException {
                tvgen = VectorUtils.createVectorGenerator(
                        Env.getProperty(GroupedVectorStore.class, "vector.type"));
                writers = new HashMap<Object, SequenceFile.Writer>();
                keyWritable = new IntWritable();
                valueWritable = new VectorWritable();
            }
        });
    }

    @Override
    public void putNext(Tuple tuple) throws IOException {
        if (tuple != null) {
            Object part = tuple.get(0);
            DataBag bag = (DataBag) tuple.get(1);
            for (Tuple t : bag) {
                int id = (Integer)t.get(0);
                Vector vector = tvgen.apply((Tuple) t.get(1));
                write(part, id, vector);
            }
        }
    }

    @Override
    protected void handleWriterClose(TaskAttemptContext ctx) throws IOException {
        close();
    }

    private void write(Object part, int id, Vector vector) throws IOException {
        SequenceFile.Writer writer = writers.get(part);
        if (writer == null) {
            Configuration conf = UDFContext.getUDFContext().getJobConf();
            Path file = PathUtils.enter(getStorePath(), String.valueOf(part), "part-" + Env.getPartID());
            writer = IOUtils.forSequenceWrite(
                    conf, file, IntWritable.class, VectorWritable.class);
            writers.put(part, writer);
        }
        keyWritable.set(id);
        valueWritable.set(vector);
        writer.append(keyWritable, valueWritable);
    }

    private void close() throws IOException {
        for (SequenceFile.Writer writer : writers.values()) {
            writer.close();
        }
        writers.clear();
        LOG.info("closed writer");
    }

    @Override
    public void checkSchema(ResourceSchema schema) throws IOException {
        SchemaUtils.claim(schema, 0,
                DataType.INTEGER,
                DataType.CHARARRAY,
                DataType.UNKNOWN); //the part name (usually will be int, but generally accept any type)
        ResourceSchema.ResourceFieldSchema bag = SchemaUtils.claim(
                schema, 1, DataType.BAG);   //the bag
        ResourceSchema.ResourceFieldSchema tuple =
                SchemaUtils.claim(bag, 0, DataType.TUPLE);  //the tuple of (id, vector)
        SchemaUtils.claim(tuple, 0, DataType.INTEGER);        //the id
        tuple = SchemaUtils.claim(tuple, 1, DataType.TUPLE);  //the vector
        Env.setProperty(GroupedVectorStore.class, "vector.type",
                VectorUtils.typeOfVector(tuple.getSchema()));
    }

}
