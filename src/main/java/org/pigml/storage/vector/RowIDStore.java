package org.pigml.storage.vector;

import com.google.common.base.Preconditions;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.ResourceSchema;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.util.UDFContext;
import org.pigml.storage.ProxyStore;
import org.pigml.udf.text.Constants;
import org.pigml.utils.Env;
import org.pigml.utils.IOUtils;
import org.pigml.utils.SchemaUtils;
import org.pigml.utils.VectorUtils;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created with IntelliJ IDEA.
 * User: g
 * Date: 13-9-26
 * Time: 下午3:28
 * To change this template use File | Settings | File Templates.
 */
public class RowIDStore extends ProxyStore implements Constants {

    static public final String MATRIX = "matrix";
    static public final String DOCINDEX = "docIndex";

    private static final Log LOG = LogFactory.getLog(RowIDStore.class);
    private int parallel;
    private TupleFactory tfac;
    private int sequence;

    public RowIDStore(final String parallel) throws IOException, ParseException, ClassNotFoundException {
        super(new VectorStore("integer"));
        this.parallel = Integer.parseInt(parallel);
        Env.inBackground(new Env.BackgroundProcedure() {
            @Override
            public void execute(Configuration conf) throws IOException {
                tfac = TupleFactory.getInstance();
                sequence = Env.getPartID();
                LOG.info("Instance "+sequence+" of "+parallel);
            }
        });
        Preconditions.checkArgument(this.parallel > 0);
    }

    @Override
    public void putNext(Tuple tuple) throws IOException {
        if (tuple == null) {
            return;
        }
        String location = Env.getProperty(RowIDStore.class, "originallocation");
        Path indexfile = new Path(location + Path.SEPARATOR + DOCINDEX, "part-index-"+sequence);
        Configuration conf = UDFContext.getUDFContext().getJobConf();
        SequenceFile.Writer indexWriter = IOUtils.forSequenceWrite(
                conf, indexfile, IntWritable.class, Text.class);
        DataBag bag = (DataBag) tuple.get(1);
        for (Tuple t : bag) {
            String name = (String) t.get(0);
            super.putNext(tfac.newTupleNoCopy(Arrays.asList(sequence, t.get(1))));
            indexWriter.append(new IntWritable(sequence), new Text(name));
            sequence += parallel;
        }
        indexWriter.close();
    }

    @Override
    public void checkSchema(ResourceSchema schema) throws IOException {
        ResourceSchema.ResourceFieldSchema bag = SchemaUtils.claim(schema, 1, DataType.BAG);
        ResourceSchema.ResourceFieldSchema tuple = SchemaUtils.claim(bag, 0, DataType.TUPLE);
        SchemaUtils.claim(tuple, 0, DataType.CHARARRAY);
        ResourceSchema.ResourceFieldSchema innerTuple = SchemaUtils.claim(tuple, 1, DataType.TUPLE);
        VectorUtils.typeOfVector(innerTuple.getSchema());
    }

    @Override
    public void setStoreLocation(String location, Job job) throws IOException {
        super.setStoreLocation(location+ Path.SEPARATOR+MATRIX, job);
        Env.setProperty(RowIDStore.class, "originallocation", location);
    }

    @Override
    public void cleanupOnFailure(String location, Job job) throws IOException {
        super.cleanupOnFailure(location+ Path.SEPARATOR+MATRIX, job);
        Path index = new Path(location + Path.SEPARATOR + DOCINDEX);
        index.getFileSystem(job.getConfiguration()).delete(index, true);
    }

    @Override
    public void cleanupOnSuccess(String location, Job job) throws IOException {
        super.cleanupOnSuccess(location+ Path.SEPARATOR+MATRIX, job);
    }
}
